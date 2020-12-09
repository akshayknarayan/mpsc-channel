use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::Mutex;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};

pub struct Sender<T> {
    mpsc_queue: Arc<MpscQueue<T>>,
    enqueue_tries_hist: Arc<Mutex<hdrhistogram::Histogram<u32>>>,
}

// Need an explicit clone mechanism so that we can reference as appropriate
impl<T> Clone for Sender<T> {
    fn clone(&self) -> Sender<T> {
        let q = self.mpsc_queue.clone();
        q.reference_producers();
        Sender {
            mpsc_queue: q,
            enqueue_tries_hist: self.enqueue_tries_hist.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.mpsc_queue.drop_producer();
        let g = self.enqueue_tries_hist.lock().unwrap();
        println!(
            "mpsc_channel enqueue tries ctr: p5 = {}, p25 = {}, p50 = {}, p75 = {}, p95 = {}, cnt = {}",
            g.value_at_quantile(0.05),
            g.value_at_quantile(0.25),
            g.value_at_quantile(0.5),
            g.value_at_quantile(0.75),
            g.value_at_quantile(0.95),
            g.len(),
        );

        let g = self.mpsc_queue.enq_deq_time_hist.lock().unwrap();
        println!(
            "mpsc_channel enq->deq time (ns): p5 = {}, p25 = {}, p50 = {}, p75 = {}, p95 = {}, cnt = {}",
            g.value_at_quantile(0.05),
            g.value_at_quantile(0.25),
            g.value_at_quantile(0.5),
            g.value_at_quantile(0.75),
            g.value_at_quantile(0.95),
            g.len(),
        );

        let g = self.mpsc_queue.inter_deq_time_hist.lock().unwrap();
        println!(
            "mpsc_channel deq->deq time (ns): p5 = {}, p25 = {}, p50 = {}, p75 = {}, p95 = {}, cnt = {}",
            g.value_at_quantile(0.05),
            g.value_at_quantile(0.25),
            g.value_at_quantile(0.5),
            g.value_at_quantile(0.75),
            g.value_at_quantile(0.95),
            g.len(),
        );
    }
}

unsafe impl<T: Send> Send for Sender<T> {}
unsafe impl<T: Sync> Sync for Sender<T> {}

impl<T> Sender<T> {
    #[inline]
    pub fn enqueue(&self, msg: T) -> Result<(), T> {
        self.mpsc_queue.enqueue(msg)
    }

    #[inline]
    pub fn enqueue_wait(&self, msg: T) {
        let mut m = Some(msg);
        let mut enq_tries_ctr = 0;
        loop {
            match self.mpsc_queue.enqueue(m.take().unwrap()) {
                Ok(_) => break,
                Err(msg) => m = Some(msg),
            }

            //std::thread::sleep(std::time::Duration::from_micros(1));
            //std::thread::yield_now();
            enq_tries_ctr += 1;
        }

        self.enqueue_tries_hist
            .lock()
            .unwrap()
            .saturating_record(enq_tries_ctr as _);
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.mpsc_queue.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub struct Receiver<T> {
    mpsc_queue: Arc<MpscQueue<T>>,
}

unsafe impl<T: Send> Send for Receiver<T> {}
unsafe impl<T: Sync> Sync for Receiver<T> {}

impl<T> Receiver<T> {
    #[inline]
    pub fn dequeue<'a>(&self, ents: &'a mut [T]) -> Result<&'a [T], ReceiverError> {
        let num = self.mpsc_queue.dequeue(ents);
        if num == 0 && self.mpsc_queue.n_producers.load(Ordering::Relaxed) == 0 {
            Err(ReceiverError::Disconnected)
        } else {
            Ok(&ents[..num])
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.mpsc_queue.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub enum ReceiverError {
    Disconnected,
}

pub fn new_mpsc_queue_pair_with_size<T>(size: usize) -> (Sender<T>, Receiver<T>) {
    let mpsc_q = Arc::new(MpscQueue::new(size));
    mpsc_q.reference_producers();
    (
        Sender {
            mpsc_queue: mpsc_q.clone(),
            enqueue_tries_hist: Arc::new(Mutex::new(
                hdrhistogram::Histogram::new_with_max(100, 2).unwrap(),
            )),
        },
        Receiver { mpsc_queue: mpsc_q },
    )
}

#[derive(Default)]
struct QueueMetadata {
    head: AtomicUsize,
    tail: AtomicUsize,
}

/// A multiproducer single consumer queue. The main difference when compared to `std::sync::mpsc` is that this
/// does not use a linked list (to avoid allocation). The hope is to eventually turn this into something that can carry
/// `Packets` or sufficient metadata to reconstruct that structure.
struct MpscQueue<T> {
    slots: usize, // Must be a power of 2
    mask: usize,  // slots - 1
    // FIXME: Watermark?
    producer: QueueMetadata,
    consumer: QueueMetadata,
    queue: Vec<UnsafeCell<MaybeUninit<T>>>,
    n_producers: AtomicUsize, // Number of consumers.
    enq_to_deq_time: AtomicU64,
    last_deq_time: AtomicU64,
    clock: quanta::Clock,
    enq_deq_time_hist: Arc<Mutex<hdrhistogram::Histogram<u32>>>,
    inter_deq_time_hist: Arc<Mutex<hdrhistogram::Histogram<u32>>>,
}

impl<T> MpscQueue<T> {
    pub fn new(size: usize) -> MpscQueue<T> {
        let slots = if size & (size - 1) != 0 {
            round_to_power_of_2(size)
        } else {
            size
        };

        MpscQueue {
            slots,
            mask: slots - 1,
            queue: (0..slots)
                .map(|_| UnsafeCell::new(MaybeUninit::zeroed()))
                .collect(),
            producer: Default::default(),
            consumer: Default::default(),
            n_producers: Default::default(),
            enq_to_deq_time: Default::default(),
            clock: quanta::Clock::new(),
            enq_deq_time_hist: Arc::new(Mutex::new(
                hdrhistogram::Histogram::new_with_max(100_000_000, 2).unwrap(),
            )),
            last_deq_time: Default::default(),
            inter_deq_time_hist: Arc::new(Mutex::new(
                hdrhistogram::Histogram::new_with_max(100_000_000, 2).unwrap(),
            )),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        let producer_head = self.producer.head.load(Ordering::Acquire);
        let consumer_tail = self.consumer.tail.load(Ordering::Acquire);

        let free = self
            .mask
            .wrapping_add(consumer_tail)
            .wrapping_sub(producer_head);
        self.slots - free
    }

    // This assumes that no producers are currently active.
    #[inline]
    pub fn reference_producers(&self) {
        self.n_producers.fetch_add(1, Ordering::AcqRel);
    }

    #[inline]
    pub fn drop_producer(&self) {
        self.n_producers.fetch_sub(1, Ordering::AcqRel);
    }

    #[inline]
    pub fn enqueue(&self, ent: T) -> Result<(), T> {
        let producers = self.n_producers.load(Ordering::Acquire);
        assert!(producers >= 1, "Insertion into a queue without producers");
        assert!(producers == 1);

        // a store here measures most recent enqueue until dequeue
        self.enq_to_deq_time
            .store(self.clock.start(), Ordering::SeqCst);

        // CAS measures the first enqueue until dequeue
        //self.enq_to_deq_time
        //    .compare_and_swap(0, self.clock.start(), Ordering::SeqCst);

        if producers == 1 {
            self.enqueue_sp(ent)
        } else {
            self.enqueue_mp(ent)
        }
    }

    // In the mp only version lots of time was being consumed in CAS.
    // We want to allow for the mp case, but there is no
    // need to waste cycles.
    #[inline]
    fn enqueue_sp(&self, ent: T) -> Result<(), T> {
        let producer_head = self.producer.head.load(Ordering::Acquire);
        let consumer_tail = self.consumer.tail.load(Ordering::Acquire);

        let free = self
            .mask
            .wrapping_add(consumer_tail)
            .wrapping_sub(producer_head);
        if free == 0 {
            return Err(ent);
        }

        let producer_next = producer_head.wrapping_add(1);
        // Reserve slots by incrementing head
        self.producer.head.store(producer_next, Ordering::Release);
        // Write to reserved slot.
        // safety:
        // because we incremented producer_head, we reserved the slot in mem.
        unsafe {
            self.enqueue_ents(producer_head, ent);
        }
        // Commit write by changing tail.
        // Once this has been achieved, update tail.
        // Any conflicting updates will wait on the previous spin lock.
        self.producer.tail.store(producer_next, Ordering::Release);
        Ok(())
    }

    #[inline]
    fn enqueue_mp(&self, ent: T) -> Result<(), T> {
        let mut producer_head;
        let mut consumer_tail;
        // First try and reserve memory by incrementing producer head.
        let insert = loop {
            producer_head = self.producer.head.load(Ordering::Acquire);
            consumer_tail = self.consumer.tail.load(Ordering::Acquire);
            let free = self
                .mask
                .wrapping_add(consumer_tail)
                .wrapping_sub(producer_head);
            if free == 0 {
                // Short circuit, no insertion
                return Err(ent);
            } else {
                let producer_next = producer_head.wrapping_add(1);
                if self
                    .producer
                    .head
                    .compare_exchange(
                        producer_head,
                        producer_next,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    )
                    .is_err()
                {
                    continue;
                }

                break 1;
            }
        };

        if insert > 0 {
            // If we successfully reserved memory, write to memory.
            let end = producer_head.wrapping_add(insert);

            // safety:
            // because we incremented producer_head, we reserved the slot in mem.
            unsafe {
                self.enqueue_ents(producer_head, ent);
            }

            // Commit write by changing tail.
            // Before committing we wait for any preceding writes to finish
            // This is important since we assume buffer is
            // always available upto commit point.
            while {
                let producer_tail = self.producer.tail.load(Ordering::Acquire);
                producer_tail != producer_head
            } {
                //pause(); // Pausing is a nice thing to do during spin locks
            }

            // Once this has been achieved, update tail.
            // Any conflicting updates will wait on the previous spin lock.
            self.producer.tail.store(end, Ordering::Release);
            Ok(())
        } else {
            Err(ent)
        }
    }

    #[inline]
    unsafe fn enqueue_ents(&self, start: usize, ent: T) {
        let mask = self.mask;
        let len = self.slots;
        let mut queue_idx = start & mask;
        // FIXME: Unroll?
        if queue_idx >= len {
            queue_idx = 0;
        }

        *self.queue[queue_idx].get() = MaybeUninit::new(ent);
    }

    #[inline]
    pub fn dequeue(&self, ents: &mut [T]) -> usize {
        let end = self.clock.end();
        let start = self.enq_to_deq_time.swap(0, Ordering::SeqCst);
        if start != 0 {
            let dur = self.clock.delta(start, end);
            self.enq_deq_time_hist
                .lock()
                .unwrap()
                .saturating_record(dur.as_nanos() as _);
        }

        let last_end = self.last_deq_time.swap(end, Ordering::SeqCst);
        let dur = self.clock.delta(last_end, end);
        self.inter_deq_time_hist
            .lock()
            .unwrap()
            .saturating_record(dur.as_nanos() as _);

        // NOTE: This is a single consumer dequeue as assumed by this queue.
        let consumer_head = self.consumer.head.load(Ordering::Acquire);
        let producer_tail = self.producer.tail.load(Ordering::Acquire);
        let available_entries = producer_tail.wrapping_sub(consumer_head);
        let dequeue = std::cmp::min(ents.len(), available_entries);
        if dequeue > 0 {
            let consumer_next = consumer_head.wrapping_add(dequeue);
            // Reserve what we are going to dequeue.
            self.consumer.head.store(consumer_next, Ordering::Release);
            // safety: we reserved the dequeue parts.
            unsafe {
                self.dequeue_ents(consumer_head, dequeue, ents);
            }
            // Commit that we have dequeued.
            self.consumer.tail.store(consumer_next, Ordering::Release);
        }

        dequeue
    }

    #[inline]
    unsafe fn dequeue_ents(&self, start: usize, dequeue: usize, mbufs: &mut [T]) {
        let mask = self.mask;
        let len = self.slots;
        // FIXME: Unroll?
        let mut mbuf_idx = 0;
        let mut queue_idx = start & mask;
        if queue_idx + dequeue >= len {
            while queue_idx < len {
                let qptr = self.queue[queue_idx].get();
                mbufs[mbuf_idx] = qptr.read().assume_init();
                *qptr = MaybeUninit::zeroed();
                mbuf_idx += 1;
                queue_idx += 1;
            }
            queue_idx = 0;
        }
        while mbuf_idx < dequeue {
            let qptr = self.queue[queue_idx].get();
            mbufs[mbuf_idx] = qptr.read().assume_init();
            *qptr = MaybeUninit::zeroed();
            mbuf_idx += 1;
            queue_idx += 1;
        }
    }
}

/// Round a 64-bit integer to its nearest power of 2.
#[inline]
pub fn round_to_power_of_2(mut size: usize) -> usize {
    size = size.wrapping_sub(1);
    size |= size >> 1;
    size |= size >> 2;
    size |= size >> 4;
    size |= size >> 8;
    size |= size >> 16;
    size |= size >> 32;
    size = size.wrapping_add(1);
    size
}

#[cfg(test)]
mod test {
    #[derive(Default, Clone)]
    struct Msg {
        sent: u64,
        _payload: Vec<u8>,
    }

    #[test]
    fn time_send() {
        let (s, r) = super::new_mpsc_queue_pair_with_size(32);

        std::thread::spawn(move || {
            let c = quanta::Clock::new();
            for _ in 0..100 {
                let m = Msg {
                    sent: c.start(),
                    _payload: vec![0u8; 16],
                };

                s.enqueue_wait(m);
            }
        });

        let c = quanta::Clock::new();
        let mut ts = hdrhistogram::Histogram::<u32>::new_with_max(1_000, 3).unwrap();
        let mut entries = vec![Msg::default(); 32];
        loop {
            match r.dequeue(&mut entries[..]) {
                Ok(empty) if empty.is_empty() => {
                    std::thread::yield_now();
                }
                Ok(entrs) => {
                    for Msg { sent, .. } in entrs {
                        let t = c.delta(*sent, c.end());
                        ts.saturating_record(t.as_micros() as _);
                    }
                }
                Err(_) => {
                    break;
                }
            }
        }

        assert_eq!(ts.len(), 100);

        println!(
            "micros\np5 = {}, p25 = {}, p50 = {}, p75 = {}, p95 = {}, cnt = {}",
            ts.value_at_quantile(0.05),
            ts.value_at_quantile(0.25),
            ts.value_at_quantile(0.5),
            ts.value_at_quantile(0.75),
            ts.value_at_quantile(0.95),
            ts.len(),
        );
    }

    #[test]
    fn send_usize() {
        let (s, r) = super::new_mpsc_queue_pair_with_size(32);

        std::thread::spawn(move || {
            for _ in 0..100 {
                s.enqueue(14).unwrap();
            }
        });

        let mut entries = vec![0; 32];
        let mut ctr = 0;
        loop {
            match r.dequeue(&mut entries[..]) {
                Ok(empty) if empty.is_empty() => {
                    std::thread::yield_now();
                }
                Ok(entrs) => {
                    for i in entrs {
                        assert_eq!(*i, 14);
                        ctr += 1;
                    }
                }
                Err(_) => {
                    break;
                }
            }
        }

        assert_eq!(ctr, 100);
    }
}
