/**************
 * $Id: AbstractUDPServer.java,v 1.1.1.1 2008-09-04 21:43:12 jablo Exp $
 * $Name: not supported by cvs2svn $
 *
 * Jacob Lorensen, TDC KabelTV, 2006
 */
package local.lorensen.jacob.jio;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.*;
import java.util.logging.Logger;
import local.lorensen.jacob.statemachine.AbstractStateMachine;
import local.lorensen.jacob.util.WatchDog;

/**
 * Abstract multithreaded, nonblocking I/O UDP Server. The purpose of the
 * AbstractUDPServer class is to ease the task of making high-performance,
 * scalable, high-reliability embeddable UDP server classes.
 * <p>
 *
 * An attempt at scalability is made in the implementation. The clas
 * uses a tunable number of worker threads that does all network I/O in
 * non-blocking mode. Each worker thread can handle an unlimited number of
 * clients. This way the number of threads can be adjusted according to
 * performance requirements and physical hardware. The default number of threads
 * is twice the number of processors in the system.
 * <p>
 *
 * @author Jacob Lorensen, 2007.
 */
public abstract class AbstractUDPServer implements Runnable, WatchDog.Guard {
    private static Logger log = Logger.getLogger(AbstractUDPServer.class.getName());
    // configuration
    private int servicePort;
    private int nThreads;
    // State
    private ThreadGroup tg;
    private SessionWorker[] workers;
    private DatagramChannel serverChannel;
    private Selector selector;
    
    /**
     * Create a UDP server
     *
     * @see AbstractUDPServer.SessionWorker
     */
    public AbstractUDPServer(int servicePort, int nThreads) throws SocketException, IOException {
        this.servicePort = servicePort;
        this.nThreads = nThreads;
        if (nThreads <= 0)
            throw new IllegalArgumentException("Cannot have nThreads <= 0");
        tg = new ThreadGroup(getClass().getName() + "-workers");
        tg.setDaemon(true);
        serverChannel = DatagramChannel.open();
        serverChannel.socket().bind(new InetSocketAddress(servicePort));
        serverChannel.configureBlocking(false);
        selector = Selector.open();
        serverChannel.register(selector, SelectionKey.OP_READ);
        workers = new SessionWorker[nThreads];
        for (int i = 0; i < nThreads; i++) {
            workers[i] = new SessionWorker(tg, "SessionWorker-" + i);
            workers[i].start();
        }
    }
    
    /**
     * Allocate a new session object to handle the UDP service
     *
     * @param w
     *            the SessionWorker that is the super class of the new session object to be created
     * @param ch
     * @return the session object
     */
    protected abstract SessionWorker.AbstractUDPSession newSession(SessionWorker w, DatagramChannel ch);
    
    /**
     * Allocate the initial ByteBuffers used to read in the initial data block.
     * To implement a protocol, you have to provide the initial data buffer to hold initial incoming packets. You
     * may opionally implement the releaseInitialBuffer() to e.g. re-use buffers instead of relying on
     * garbage collection.
     *
     * @see #releaseInitialBuffer
     */
    protected abstract ByteBuffer allocInitialBuffer();
    
    /**
     * Release for later use an allocated ByteBuffer (that comes from allocInitialBuffer).
     * Default implementation does nothing, relying on Java VM garbage collection.
     *
     * @see #allocInitialBuffer
     */
    protected void releaseInitialBuffer(ByteBuffer buf) {
        return;
    }
    
    // The WatchDog.Guard interface
    /**
     * What are we monitoring
     */
    public String guardText() {
        return "UDPServer/internals";
    }
    
    /**
     * Run through internal data structures, thread groups, threads and check
     * consistency.
     *
     * This function can be called periodically from the main connection to
     * monitor internal workings of the threads in the Tftp server.
     * <p>
     *
     * Checks:
     * <ul>
     * <li>That all worker threads are alive</il>
     * <li>That selector is open</li>
     * <li>That serverChannel is open and connected</li>
     * </ul>
     *
     * @return True if everything is ok, False otherwise.
     */
    public boolean guardCheck() {
        for (int i = 0; i < nThreads; i++)
            if (!workers[i].isAlive())
                return false;
        return selector.isOpen() && serverChannel.isOpen();
    }
    
    /**
     * Fix anything that may be wrong and causes checkHealth to return False.
     *
     * This may be poor coding---I don't think I am able to test this code!!!
     * <p>
     *
     * This code tries to
     * <ul>
     * <li>Re-start any dead worker threads
     * <li>Re-open the server socket if closed/disconnected, including
     * re-setting the selector
     * <li>Re-open the selector
     * </ul>
     */
    public void guardRepair() {
        try {
            for (int i = 0; i < nThreads; i++) {
                if (!workers[i].isAlive()) {
                    workers[i].join(1000);
                    log.warning("Restarted AbstractUDPServer WorkerThread " + i);
                    workers[i] = new SessionWorker(tg, "SessionWorker-" + i);
                    workers[i].start();
                }
            }
            if (!serverChannel.isOpen()) {
                serverChannel = DatagramChannel.open();
                serverChannel.socket().bind(new InetSocketAddress(servicePort));
                serverChannel.configureBlocking(false);
                // cancel all open selections (should be only 1, the server
                // channel)
                for (Iterator it = selector.keys().iterator(); it.hasNext();) {
                    SelectionKey sk = (SelectionKey) it.next();
                    sk.cancel();
                }
                // re-establish the new opened channel selector
                serverChannel.register(selector, SelectionKey.OP_READ);
            }
            if (!selector.isOpen()) {
                selector = Selector.open();
                serverChannel.register(selector, SelectionKey.OP_READ);
            }
        } catch (InterruptedException e) {
            log.severe("Caught exception repaiing TftpListener " + e);
        } catch (IOException e) {
            log.severe("Caught exception repaiing TftpListener " + e);
        }
    }
    
    /**
     * flag to control when the service should stop
     */
    private boolean stop;
    
    /**
     * The main loop of the UDP server.
     *
     * Waits for new connections and delegates the connections to one of the
     * active SessionWorker objects
     */
    public void run() {
        Thread.currentThread().setName("AbstractUDPServer:" + servicePort);
        stop=false;
        try {
            while (selector.select() >= 0) {
                if (stop) break;
                // New client
                Iterator i = selector.selectedKeys().iterator();
                i.next();
                i.remove();
                try {
                    DatagramChannel clientChannel;
                    SocketAddress clientAdr;
                    ByteBuffer buf = allocInitialBuffer();
                    // Create a new socket for this client
                    clientAdr = serverChannel.receive(buf);
                    buf.flip();
                    if (duplicateConnect(clientAdr)) {
                        log.info("Ignoring duplicate connect from " + clientAdr);
                        continue;
                    }
                    clientChannel = DatagramChannel.open();
                    clientChannel.connect(clientAdr);
                    clientChannel.configureBlocking(false);
                    log.info("New client " + clientAdr);
                    handleClient(new NewClientPair(clientChannel, buf));
                } catch (IOException e) {
                    log.warning(e + " ignoring");
                }
            }
        } catch (ClosedSelectorException e) {
        } catch (IOException e) {
            log.severe("Exception " + e + ", giving up");
            e.printStackTrace(System.err);
        }
        // We've been requested to stop. Shutdown cleanly:
        try {
            selector.close();
        } catch (IOException ex) {
            log.warning("Error closing server selector " + ex);
            ex.printStackTrace(System.err);
        }
        try {
            serverChannel.close();
        } catch (IOException ex) {
            log.warning("Error closing server socket " + ex);
            ex.printStackTrace(System.err);
        }
        // Stop any active workers
        for (int i=0; i<workers.length; i++) {
            workers[i].stopWorker();
            try {
                workers[i].join();
            } catch (InterruptedException ex) {
                log.warning("Interrupted while waiting for " + workers[i] + " to terminate: " + ex);
                ex.printStackTrace();
            }
        }
        // Tell our concrete class that the service is stopping.
        shutdownService();
    }
    
    /**
     * Stop the service
     */
    public final void terminateService() {
        // Stop the main loop
        log.info("Terminating UDP service " + this);
        stop = true;
        selector.wakeup();
    }
    
    /**
     * Perform any operations necessary to force the service down in a clean fashion; override
     */
    protected abstract void shutdownService();
    
    /**
     * Re-configure the number of worker threads
     */
    public final void setNumberOfWorkerThreads(int n) {
        throw new UnsupportedOperationException("Operation not implemented yet");
    }
    
    public final int getNumberOfWorkerThreads() {
        return nThreads;
    }
    
    /**
     * Checks if a client is already being connected to the server
     *
     * @param clientAddress
     *            A SocketAddress of the remote end.
     * @return true if this clientAddress is already handled by one of the
     *         SessionWorkers false otherwise.
     */
    private boolean duplicateConnect(SocketAddress clientAddress) {
        for (int i = 0; i < nThreads; i++)
            if (workers[i].isActiveClient(clientAddress))
                return true;
        return false;
    }
    
    private int nxtThread = 0;
    
    /**
     * Schedule a new client on one of the worker threads
     *
     * @param p
     *            A DatagramChannel and the first block read for a new incoming client connection
     */
    synchronized private void handleClient(NewClientPair p) {
        log.fine("Giving new client to thread " + workers[nxtThread]);
        workers[nxtThread].addClient(p);
        nxtThread++;
        if (nxtThread >= nThreads)
            nxtThread = 0;
    }
    
    /**
     * Perform any cleanup necessary after a connection is finished.
     *
     * @param ch
     *            The DatagramChannel that was used to communicate with the
     *            client.
     */
    synchronized protected void cleanupClient(DatagramChannel ch) {
        try {
            ch.close();
        } catch (IOException e) {
            log.warning("Caught exception " + e + " while cleaning up connection");
        }
    }
    
    /**
     * Holds a new client channel and the first buffer read.
     * We only use this class because we need to read the first packet to determine the client's
     * address and I cannot find a way to "peek" at a data packet without reading it
     * when using non-blocking I/O.<p>
     * &mdash&JLo.
     */
    private class NewClientPair {
        DatagramChannel ch;
        ByteBuffer buf;
        public NewClientPair(DatagramChannel ch, ByteBuffer buf) {
            this.ch=ch; this.buf=buf;
        }
    }
    
    /**
     * A daemon thread to handle an unlimited number of Tftp sessions using
     * nonblocking I/O
     */
    public class SessionWorker extends Thread {
        // State
        private Selector selector; // Selector for active sessions' sockets
        /** Active sessions handled by this thread, indexed by Datagramchannel */
        private Map clients;
        /** New&mdash;as yet unhandled&mdash;clients posted from AbstractUDPSession main loop */
        private Stack newClients;
        /** Active sessions that have requested rescheduling */
        private Stack postedSessionInputPairs;
        
        /**
         * Creates the SessionWorker as a daemon thread.
         *
         * @param tg
         *            The thread group that the daemon thread should be part of
         * @param tn
         *            The name of the thread
         */
        private SessionWorker(ThreadGroup tg, String tn) throws IOException {
            super(tg, tn);
            setDaemon(true);
            clients = Collections.synchronizedMap((new HashMap()));
            newClients = new Stack();
            postedSessionInputPairs = new Stack();
            selector = Selector.open();
        }
        
        /**
         * Checks if a client's SocketAddress is already registered.
         * Synchronized to protect clients HashMap from updates while
         * isActiveClient iterates
         *
         * @param clAdr
         *            the client's SocketAddress
         * @return true if the SocketAddress is already registered, false
         *         otherwise
         */
        private synchronized boolean isActiveClient(SocketAddress clAdr) {
            for (Iterator i = clients.values().iterator(); i.hasNext();) {
                AbstractUDPSession cl = (AbstractUDPSession) i.next();
                if (cl.getRemoteSocketAddr().equals(clAdr))
                    return true;
            }
            return false;
        }
        
        /**
         * Adds a new client to the clients handled by this SessionWorker.
         * Synchronized to protect clients HashMap from updates while
         * isActiveClient iterates
         *
         * @param p
         *            The DatagramChannel and first buffer read from the UDP connection
         *            with the client.
         * @see #isActiveClient
         */
        private synchronized void addClient(NewClientPair p) {
            AbstractUDPSession cl = newSession(this, p.ch);
            clients.put(p.ch, cl);
            newClients.push(p);
            selector.wakeup();
        }
        
        /**
         * Does the internal bookkeping and cleanup to terminate a client
         * connection. Synchronized to protect clients HashMap from updates
         * while isActiveClient iterates
         *
         * @param ch
         *            DatagramChannel representing the connection with the
         *            client.
         * @see #isActiveClient
         */
        protected synchronized void cleanupClient(DatagramChannel ch) {
            clients.remove(ch);
            AbstractUDPServer.this.cleanupClient(ch);
        }
        
        /**
         * Postinput to a session.
         *
         * Used by a Session object when it needs to induce input to the state machine e.g.
         * in connection
         * with a timeout or other external event that would not by itself
         * wakeup the select().
         * <p>
         *
         * Synchronized to protect clients HashMap from updates while
         * isActiveClient iterates
         *
         * @param ses
         *            The Session object that requests re-scheduling.
         */
        synchronized private void postInput(AbstractUDPSession ses, Object o) {
            // An inactive/terminated session should NOT be able to
            // reschedule itself
            if (clients.containsValue(ses)) {
                postedSessionInputPairs.push(new SessionInputPair(ses,o));
                selector.wakeup();
            } else
                log.severe(ses + ": Session was terminated but tried to re-schedule itself.");
        }
        
        private class SessionInputPair {
            private AbstractUDPSession ses;
            private Object o;
            public SessionInputPair(AbstractUDPSession ses, Object o) {
                this.ses=ses; this.o=o;
            }
        }
        
        private boolean stop = false;
        
        /**
         * Main SessionWorker loop.
         *
         * Waits in select() for new data to arrive on one of client data
         * sockets. The select() will be interrupted (wakeup) by addClient() or
         * postInput().
         * <p>
         *
         * Then if there are any new clients they are added to he list of active
         * clients and their firstPacket() method is called. If any child
         * Sessions have requested a re-schedule their schedule()
         * method is called.
         */
        public void run() {
            try {
                while (selector.select() >= 0) {
                    if (stop) break;
                    // Handle all outstanding requests/data packets
                    for (Iterator i = selector.selectedKeys().iterator(); i.hasNext(); i.remove()) {
                        SelectionKey sk = (SelectionKey) i.next();
                        DatagramChannel ch = (DatagramChannel)sk.channel();
                        AbstractUDPSession ses = (AbstractUDPSession) clients.get(sk.channel());
                        ByteBuffer bb = ses.allocBuffer();
                        // Read incoming data packet
                        try {
                            bb.clear();
                            ch.receive(bb);
                            bb.flip();
                        } catch (IOException e) {
                            ses.input(e);
                            continue;
                        }
                        ses.input(bb);
                        ses.releaseBuffer(bb);
                    }
                    // Handle new connections
                    while (!newClients.empty()) {
                        try {
                            NewClientPair p = (NewClientPair) newClients.pop();
                            p.ch.register(selector, SelectionKey.OP_READ);
                            AbstractUDPSession ses = (AbstractUDPSession)clients.get(p.ch);
                            ses.initialize();
                            ses.input(p.buf);
                            releaseInitialBuffer(p.buf);
                        } catch (EmptyStackException e) {
                            log.severe("Exception while registering new clients. " + e);
                            e.printStackTrace(System.err);
                        }
                    }
                    // Handle sessions with input posted
                    while (!postedSessionInputPairs.empty()) {
                        try {
                            SessionInputPair p = (SessionInputPair) postedSessionInputPairs.pop();
                            p.ses.input(p.o);
                        } catch (EmptyStackException e) {
                            log.severe("Exception while rescheduling. " + e);
                            e.printStackTrace(System.err);
                        }
                    }
                }
            } catch (ClosedSelectorException e) {
                //log.info("Selector closed");
            } catch (IOException e) {
                log.severe("Caught exception " + e + " giving up");
                e.printStackTrace(System.err);
            }
            // We've been asked to stop.
            try {
                selector.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            //Clean up clients.
            // Build a stack with all clients, so each client can remove itself from the clients map without
            // breaking the iterator. Note: Due to the synchronous nature of each worker thread and session object,
            // We are certain that no clients will be removed between building the
            // stack and calling shutdownSession().
            log.info(this+ ": Stopping all sessions");
            Stack s = new Stack();
            for (Iterator i=clients.values().iterator();  i.hasNext(); ) {
                AbstractUDPSession client = (AbstractUDPSession)i.next();
                s.push(client);
            }
            // Now ask each active session in turn to terminate itself
            for (Iterator i=s.iterator(); i.hasNext(); ) {
                AbstractUDPSession client = (AbstractUDPSession)i.next();
                log.info(this+ ": Stopping session "+ client);
                client.shutdown();
            }
        }
        
        private void stopWorker() {
            log.info("Stopping worker " + this);
            stop=true;
            selector.wakeup();
        }
        
        /*
         * Session Implements the abstract state machine of a single UDP
         * protocol session.
         */
        public abstract class AbstractUDPSession extends AbstractStateMachine {
            protected DatagramChannel connection; // UDP client Connection
            
            /**
             * Create a Session object.
             *
             * @param connection
             *            The DatagramChannel, already open, that represents the
             *            connection with the client.
             */
            protected AbstractUDPSession(DatagramChannel connection) {
                this.connection = connection;
            }
            
            public String toString() {
                return "udpService@" + getRemoteSocketAddr().toString();
            }
            
            /**
             * @return The InetAddress of the client connection.
             */
            public InetAddress getClientInetAddr() {
                return connection.socket().getInetAddress();
            }
            
            /**
             * @return The SocketAddress of the client connection.
             */
            public SocketAddress getRemoteSocketAddr() {
                return connection.socket().getRemoteSocketAddress();
            }
            
            /**
             * Terminate a session and clean up
             */
            protected void cleanupClient() {
                SessionWorker.this.cleanupClient(connection);
            }
            
            /**
             * Post input to this session, can be called from within the state machines.
             */
            protected final synchronized void postInput(Object o) {
                SessionWorker.this.postInput(this, o);
            }
            
            /**
             * Allocate a ByteBuffer to hold the next packet read/write
             * @return a ByteBuffer
             */
            protected abstract ByteBuffer allocBuffer();
            
            /**
             * Release a ByteBuffer after use. This provides an optional hoook to re-use ByteBuffers
             * instead of relying on garbage collection.
             * @param buf the ByteBuffer that will nomore be used.
             */
            protected void releaseBuffer(ByteBuffer buf) {
            }
        }
    }
}
