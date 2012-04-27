/****************
 * $Id: WorkQueue.java,v 1.2 2009-01-07 13:31:27 jablo Exp $
 * $Name: not supported by cvs2svn $
 *
 * Unbounded work queue of "Runnable"s.
 *
 * (C) Jacob Lorensen, TDC KabelTV, 2006.
 *****************/
package local.lorensen.jacob.util;

import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Unbounded work queue with a configurable max number of worker threads.
 * <p>
 *
 * Usage: Creating a WorkQueue(int <em>nthr</em>), starts up to <em>nthr</em>
 * threads to work taking jobs from the queue. Initially one thread is created.
 * Calling addWork(Runnable <em>r</em>) places <em>r</em> on the work
 * queue. If job queue length exceeds a maximum, additional threads will be
 * created up to the maximum number of threads allowed. Work will be executed in
 * the order they are added to the queue (FIFO). Objects on the queue must
 * implement the Runnable interface.
 * <p>
 *
 * Based on SUN example of unbounded work queue, heavily changed.
 * <p>
 * TODO: Replace <code>synchronized (&hellip;)</code> and <code>synchronized(this)</code> with
 * lock on <code>queue</code> instead.
 *
 * @author Jacob Lorensen, jalor@tdc.dk, TDC KabelTV, 2006, 2007
 */
public class WorkQueue {
    private static final Logger log = Logger.getLogger(WorkQueue.class.getName());
    private String name;
    private LinkedList queue, workers;
    private int maxThreads;
    private int maxjobs; // threshold number of jobs that trigger a new thread to be started.
    private volatile boolean closing;

    public WorkQueue() {
        this(1);
    }

    /**
     * Create a WorkQueue with a number of worker threads executing jobs from
     * the workqueue.
     *
     * @param maxThreads
     *            The maximum number of worker threads. Threads are created when
     *            the work queue length exceeds default 1 elements.
     */
    public WorkQueue(int maxThreads) {
        this(maxThreads, 1);
    }

    public WorkQueue(String name, int maxThreads) {
        this(name, maxThreads, 1);
    }

    /**
     * Create a WorkQueue with number of worker threads executing jobs from the
     * workqueue.
     *
     * @param maxThreads
     *            The maximum number of worker threads.
     * @param maxjobs
     *            The length of the work queue that triggers creation of an
     *            additional worker thread.
     */
    public WorkQueue(int maxThreads, int maxjobs) {
        this(null, maxThreads, maxjobs);
    }

    public synchronized int getThreads() {
        if (workers == null)
            return -1;
        return workers.size();
    }

    /**
     * Create a WorkQueue with number of worker threads executing jobs from the
     * workqueue.
     *
     * @param name
     *            The name of the work queue (used for logging).
     * @param maxThreads
     *            The maximum number of worker threads.
     * @param maxjobs
     *            The length of the work queue that triggers creation of an
     *            additional worker thread.
     */
    public WorkQueue(String name, int maxThreads, int maxjobs) {
        if (maxjobs < 1) {
            throw new IllegalArgumentException("Maxjobs must be >= 1");
        }
        if (maxThreads < 1) {
            throw new IllegalArgumentException("MaxThreads must be >= 1");
        }
        this.name = (name == null) ? ("default") : (name);
        this.maxjobs = maxjobs;
        this.maxThreads = maxThreads;
        queue = new LinkedList();
        workers = new LinkedList();
        closing = false;
        addWorkerThread();
    }

    @Override
    public String toString() {
        return "WorkQueue(" + name + ")";
    }

    protected synchronized boolean testAndSetClosing() {
        if (closing) {
            return true;
        }
        this.closing = true;
        return false;
    }

    protected synchronized boolean isClosing() {
        return closing;
    }

    public void close() {
        if (testAndSetClosing()) {
            return;
        }
        while (workers != null && workers.size() > 0) {
            try {
                Worker t = (Worker) workers.removeLast();
                t.setStop();
                log.log(Level.FINE, "{0} Notifying threads about close", this);
                synchronized (this) {
                    this.notifyAll();
                }
                try {
                    log.log(Level.FINE, "Waiting for thread {0} to terminate", t);
                    t.join();
                    log.log(Level.FINER, "Thread {0} terminated or wait timeout...", t);
                } catch (InterruptedException ex) {
                    log.log(Level.FINE, "Interrupted waiting for thread {0} to terminate", t);
                    ex.printStackTrace(System.err);
                }
            } catch (NoSuchElementException e) {
                continue;
            }
        }
        maxThreads = 0;
        queue = null;
        workers = null;
    }

    /**
     * Add an additional worker thread to the work queue
     */
    private synchronized void addWorkerThread() {
        log.log(Level.INFO, "{0}: Adding worker thread: {1}", new Object[]{this, workers.size() + 1});
        Thread t = new Worker(this + "-Worker-" + (workers.size() + 1));
        workers.add(t);
        t.start();
    }

    /**
     * Change the maximum number of allowable worker threads.
     *
     * @param maxThreads
     *            new max. number of threads.
     */
    public synchronized void setMaxThreads(int maxThreads) {
        if (maxThreads <= 0) {
            throw new IllegalArgumentException("Number of WorkQueue threads must be positive");
        }
        if (this.maxThreads == maxThreads) {
            log.log(Level.FINEST, "{0}: Max number of threads unchanged ({1})", new Object[]{this, maxThreads});
            return;
        }
        this.maxThreads = maxThreads;
        log.log(Level.FINE, "{0}: Changed max. number of worker threads to {1}", new Object[]{this, maxThreads});
    }

    /**
     * Return the maximum number of allowable worker threads.
     */
    public synchronized int getMaxThreads() {
        return maxThreads;
    }

    /**
     * Add work to the work queue. Increase the number of worker threads if
     * queue length exceeds threshold in maxjobs.
     *
     * @param r
     *            A Runnable object carrying the work load.
     */
    public synchronized void addWork(Runnable r) {
        if (queue == null || workers == null) {
            throw new RuntimeException("Queue has been closed");
        }
        log.log(Level.FINER, "{0}: Adding work {1}", new Object[]{this, r});
        if (queue.indexOf(r) != -1) {
            throw new IllegalArgumentException(this + ": Can't add the same job " + r + " twice to a WorkQueue");
        }
        if (workers.size() == 0 || (queue.size() >= maxjobs && workers.size() < maxThreads)) {
            addWorkerThread();
        }
        queue.addLast(r);
        notify();
    }

    /**
     * Retrieve work from the work queue; block if the queue is empty. Also checks
     * for maximum number of active threads and terminates if there are too many
     * active threads.<p>
     * If the job retrieved is the last job in the queue, <code>waitEmpty</code> is notifyAll()ed
     * unblocking any threads that may be waiting for the queue to empty.
     *
     * @return Returns the next available Runnable target on the work queue,
     *         blocking until one is available. Or null, if the thread should
     *         terminate due to there being more active threads than requested.
     */
    private Runnable getWork() throws InterruptedException {
        Runnable newJob = null;
        boolean isEmpty = false;
        synchronized (this) {
            try {
                while (queue.isEmpty() && !closing) {
                    this.wait();
                    log.log(Level.FINER, "{0} was notiofied; Q;{1}", new Object[]{this, queue.size()});
                }
            } catch (InterruptedException e) {
                log.log(Level.FINE, "This {0} was interrupted while waiting for job", Thread.currentThread());
                throw e;
            }
            if (maxThreads < workers.size()) {
                if (!workers.remove(this)) {
                    log.log(Level.WARNING, "Thread removed that was not in thread list?!?!?!?? {0}", this);
                }
                log.log(Level.FINE, "Marking thread {0} for termination due to too many active threads", this);
                return null;
            }
            if (!closing)
                newJob = (Runnable) queue.removeFirst();
            else
                newJob = null;
            isEmpty = queue.isEmpty();
        }
        synchronized (waitEmpty) {
            if (isEmpty)
                waitEmpty.notifyAll();
        }
        return newJob;
    }

    /**
     * Remove a work load from the work queue.
     *
     * @param r
     *            The object to remove
     */
    public void removeWork(Runnable r) {
        boolean isEmpty = false;
        synchronized (this) {
            while (queue.remove(r)) {
                isEmpty = queue.isEmpty();
            }
        }
        synchronized (waitEmpty) {
            if (isEmpty)
                waitEmpty.notifyAll();
        }
    }

    /**
     * Return the length of the work queue
     *
     * @return length of the work queue
     */
    public int size() {
        if (queue == null)
            return -1;
        return queue.size();
    }
    private final Object waitEmpty = new Object();

    /**
     * Waits until the WorkQueue is empty, ie. has no more waiting jobs to run.
     * When this method returns, the WorkQueue has been empty; there is no guarantees
     * though that the queue is still empty once control is traferred to the caller.
     * Thas is, if multiple threads add jobs to the queue and one or more threads
     * execute waitTillEmpty, then all of the waiting threads will return once
     * the queue is empty&mdash;<b>but</b> other threads may put job into the
     * queue in the time between the queue is detected empty and the waitTillEmpty()
     * method returns.
     * <p>
     * This method thus mostly has a meaning if a single thread adds jobs to the work queue
     * and this thread wants to wait until the queue is empty  before continuing.
     * @returns returns when the WorkQueue has been detected to be empty.
     */
    public void waitTillEmpty() {
        synchronized (waitEmpty) {
            while (size() != 0) {
                try {
                    waitEmpty.wait();
                    return;
                } catch (InterruptedException ex) {
                    Logger.getLogger(WorkQueue.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    /**
     * The threads that execute work from the work queue
     */
    private class Worker extends Thread {
        private volatile boolean stop;

        Worker(String n) {
            super(n);
            stop = false;
            this.setDaemon(true);
        }

        /**
         * Flag this thread for termination.
         */
        public synchronized void setStop() {
            log.log(Level.FINE, "{0}: Requested to stop.", this);
            stop = true;
//            this.interrupt();
        }

        public synchronized boolean getStop() {
            return stop;
        }

        @Override
        public void run() {
            while (!getStop()) {
                Runnable r = null;
                try {
                    log.log(Level.FINEST, "{0} tries to get a job", this);
                    r = getWork();
                    if (r == null) {
                        break;
                    }
                    log.log(Level.FINEST, "{0} runs {1}", new Object[]{this, r});
                    r.run();
                    log.log(Level.FINEST, "{0} done with {1}", new Object[]{this, r});
                } catch (InterruptedException e) {
                    log.log(Level.INFO, "Thread interrupted, stop={0}", stop);
                } catch (Throwable e) {
                    String s;
                    if (r == null) {
                        s = "null";
                    } else {
                        s = r.toString();
                    }
                    log.log(Level.WARNING, "{0}: {1} caught exception {2}", new Object[]{this, s, e});
                    e.printStackTrace(System.err);
                }
            }
            log.log(Level.INFO, "{0}: Worker thread {1} terminating.", new Object[]{this, this.toString()});
        }
    }
}
