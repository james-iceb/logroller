use {
    crossbeam_channel::{bounded, Receiver, TrySendError},
    std::io,
};

type Task = Box<dyn FnOnce() + Send + 'static>;

const TASK_QUEUE_LIMIT: usize = 1024;

#[derive(Debug)]
pub enum SubmitError {
    QueueFull,
    Disconnected,
}

/// A fixed-size thread pool that processes compression and cleanup tasks
/// submitted by LogRoller instances via a shared channel.
pub struct ThreadPool {
    sender: crossbeam_channel::Sender<Task>,
}

impl ThreadPool {
    /// Create a new thread pool with `n_workers` persistent worker threads.
    pub fn new(n_workers: usize) -> io::Result<Self> {
        let (sender, receiver) = bounded::<Task>(TASK_QUEUE_LIMIT);
        for i in 0 .. n_workers {
            let rx: crossbeam_channel::Receiver<Task> = receiver.clone();
            std::thread::Builder::new()
                .name(format!("logroller-pool-{i}"))
                .spawn(move || {
                    while let Ok(task) = rx.recv() {
                        task();
                    }
                })
                .map_err(|err| io::Error::other(format!("logroller: failed to spawn worker thread: {err}")))?;
        }
        Ok(Self { sender })
    }

    pub fn has_capacity(&self) -> bool { !self.sender.is_full() }

    /// Submit a task to the pool. Returns a [`Receiver`] that yields `()` when
    /// the task has finished, allowing callers to optionally wait for
    /// completion.
    pub fn submit<F: FnOnce() + Send + 'static>(&self, f: F) -> Result<Receiver<()>, SubmitError> {
        let (done_tx, done_rx) = crossbeam_channel::bounded(1);
        let task: Task = Box::new(move || {
            f();
            let _ = done_tx.send(());
        });
        self.sender.try_send(task).map_err(|err| match err {
            TrySendError::Full(_) => SubmitError::QueueFull,
            TrySendError::Disconnected(_) => SubmitError::Disconnected,
        })?;
        Ok(done_rx)
    }
}
