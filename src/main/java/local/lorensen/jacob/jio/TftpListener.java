/**************
 * $Id: TftpListener.java,v 1.1.1.1 2008-09-04 21:43:12 jablo Exp $
 * $Name: not supported by cvs2svn $
 *
 * Jacob Lorensen, TDC KabelTV, 2006
 */
package local.lorensen.jacob.jio;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Logger;
import java.util.prefs.Preferences;
import local.lorensen.jacob.jio.AbstractUDPServer.SessionWorker.AbstractUDPSession;
import local.lorensen.jacob.statemachine.AbstractStateMachine;

/**
 * TftpListener implements a TFTP server. The implementation is flexible&mdash;it is meant to be
 * used as a library or object in a main application. Files or data to be transferred are opened
 * using an asynchronous callout/callback interface to the main application.
 * <p>
 * The application has full control over which files/data are opened and sent to or received from a
 * Tftp client via callout functions.
 * <p>
 * The TftpListener class implements the Runnable interface. To actually start the TFTP server a
 * thread must be created to schedule it or its <i>run()</i> method should be called. TFTP Protocol
 * Features implemented:
 * <ul>
 * <li>READ and WRITE requests</li>
 * <li>BINARY transfer mode</li>
 * <li>NETASCII transfer mode (only on MS-DOS/WINDOWS and Unix like platforms)</li>
 * <li>BlockSize option</li>
 * </ul>
 *
 * @author Jacob Lorensen, 2006.
 * @see TftpListener.SessionDone
 * @see TftpListener.TFTPSession
 * @see <a href="http://www.faqs.org/rfcs/rfc1350.html">RFC 1350, TFTP Protocol (Revision 2)</a>
 * @see <a href="http://www.faqs.org/rfcs/rfc2348.html">RFC 2348, TFTP Blocksize Option</a>
 * @see <a href="http://www.faqs.org/rfcs/rfc2347.html">RFC 2347, TFTP Option Extension</a>
 */
public abstract class TftpListener extends AbstractUDPServer {
    private static Preferences pkgprefs = Preferences.userNodeForPackage(TftpListener.class);
    private static Preferences prefs = pkgprefs.node("TftpListener");
    private static Logger log = Logger.getLogger(TftpListener.class.getName());
    // configuration
    private long rexmtmo; // default msec of TFTP protocol packet re-transmit timeout
    private int maxtmos; // default max number of timeouts before giving up
    private Timer timer;
    
    /**
     * Creates a TftpListener taking default parameters from Preferences.
     *
     * @throws IOException
     * @throws SocketException
     */
    public TftpListener() throws SocketException, IOException {
        this(prefs.getInt("TftpServicePort", 69));
    }
    
    /**
     * Creates a TftpListener taking default timeout parameters from Preferences.
     * @param port UDP port number for the server port
     * @throws IOException
     * @throws SocketException
     */
    public TftpListener(int port) throws SocketException, IOException {
        this(port,
                prefs.getInt("TftpWorkerThreads", 2 * Runtime.getRuntime().availableProcessors()),
                prefs.getInt("TftpRexmtTimeout", 2000),
                prefs.getInt("TftpMaxTimeouts", 8));
    }
    
    /**
     * Creates a TftpListener.
     * @param port UDP port number for the server port
     * @param nThreads number of worker threads serving tftp sessions
     * @param rexmtmo receice/transmit timeout in milliseconds
     * @param maxtmos maximum number of consecutive timeouts to allow before session is deemed dead.
     * @throws IOException
     * @throws SocketException
     */
    public TftpListener(int port, int nThreads, int rexmtmo, int maxtmos) throws SocketException, IOException {
        super(port, nThreads);
        this.rexmtmo = rexmtmo;
        if (rexmtmo <= 0) {
            rexmtmo = 2000;
            log.warning("Illegal TftpRexmtTimeout set, using 2000");
        }
        this.maxtmos = maxtmos;
        if (maxtmos <= 0) {
            maxtmos = 8;
            log.warning("Illegal TftpMaxTimeouts set, using 8");
        }
        timer = new Timer();
    }
    
    @Override
    protected AbstractUDPSession newSession(AbstractUDPServer.SessionWorker w, DatagramChannel ch) {
        return new TFTPSession(w, ch);
    }
    
    /**
     * Allocate the initial ByteBuffers used to read in the initial data block.
     * @see #releaseInitialBuffer
     */
    @Override
    protected ByteBuffer allocInitialBuffer() {
        return ByteBuffer.allocate(512);
    }
    
    @Override
    protected void shutdownService() {
        timer.cancel();
    }
    
    /**
     * Open a file to send to a Tftp client on behalf of the TFTPSession object. The InputStream
     * should supply the data that will be be sent to a Tftp client. Users of TftpListener class
     * must override this method.
     * <p>
     * The interface is asynchronous&mdash;openRead may return before the file is opened. Instead,
     * when the file is opened the newly opened InputStream must be passed back to the TFTP Session
     * object through a call to the openFileRead_cb method
     * <p>
     * The time between calls to openRead() method and the callback to openFileRead_cb should be
     * within TFTP protocol timeouts. Otherwise the client waiting for the connection to be
     * established may time out.
     *
     * @param ses
     *            The Tftp session object requesting opening of a file
     * @param name
     *            The file name to be opened
     * @see TftpListener.TFTPSession#openFileRead_cb
     */
    public abstract void openRead(TFTPSession ses, String name);
    
    /**
     * Open a file to receive from a Tftp client on behalf of the Session object. The OutputStream
     * will receive the data that is received from the a Tftp client. Users of TftpListener class
     * must override this method.
     * <p>
     * The interface is asynchronous&mdash;openWrite may return before the file is opened. Instead,
     * when the file is opened the newly opened OutputStream must be passed back to the TFTP Session
     * object through a call to the openFileWrite_cb method
     * <p>
     * The time between calls to openWrite() method and the callback to openFileWrite_cb should be
     * within Tftp client timeouts. Otherwise the client waiting for the connection to be
     * established will time out.
     *
     * @param ses
     *            The Tftp session object requesting opening of a file
     * @param name
     *            The file name to be opened
     * @see TftpListener.TFTPSession#openFileWrite_cb
     */
    public abstract void openWrite(TFTPSession ses, String name);
    
    /**
     * Interface used by Session's to callback to the main application when a session terminates.
     */
    public interface SessionDone {
        /**
         * Callout to the main application/creator of the Tftp server to signal that the Session
         * "ses" is done. The InputStream or OutputStream object passed from openRead() will be
         * closed by the TftpListener object when this method returns. This function is called
         * solely to communicate the state/reason for closing the client connection.
         *
         * @param ses
         *            The Tftp session object requesting opening of a file
         * @param reason
         *            The reason to close: 0=File was sent/received successfully; 1=Timeout; 2=Error
         */
        public void sessionDone(TFTPSession ses, int reason);
    }
    
    public void setTimeoutParameters(int a, int b) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
    
    /**
     * TFTPSession Implements the state machine of a single Tftp session.
     */
    public class TFTPSession extends AbstractUDPSession {
        private TimerTask timeoutHandler;
        
        /**
         * Create a Session object.
         *
         * @param cnct
         *            The DatagramChannel, already open, that represents the connection with the
         *            client.
         * @param w
         *            An AbstractUDPServer.SessionWorker object that provides the dynamic scoping context
         *            for the super class of the TFTPSession object we're creating
         */
        private TFTPSession(AbstractUDPServer.SessionWorker w, DatagramChannel cnct) {
            w.super(cnct);
            timeoutHandler = null;
            rexmtmo = TftpListener.this.rexmtmo;
            maxtmos = TftpListener.this.maxtmos;
        }
        
        @Override
        public String toString() {
            return "Tftp" + connection.socket().getRemoteSocketAddress().toString();
        }
        
        @Override
        protected AbstractStateMachine.State initialState() {
            return initialState;
        }
        
        @Override
        protected AbstractStateMachine.State shutdownState() {
            return shutdownState;
        }
        
        /**
         * Override session timeout parameters. Can be called from e.g. openFileRead() method.
         *
         * @param tmout
         *            Timeout in milliseconds
         * @param maxcount
         *            Maximum number of timeouts before giving up session
         */
        public void setTimeoutParams(int tmout, int maxcount) {
            if (tmout <= 0)
                throw new IllegalArgumentException("Retransmit timeout must be > 0");
            if (maxcount <= 0)
                throw new IllegalArgumentException("Max consecutive retransmit count must be > 0");
            rexmtmo = tmout;
            maxtmos = maxcount;
        }
        
        /**
         * Allocate a buffer to hold an incoming UDP datagram:
         * block size + 2 bytes for the command and 2 bytes for an ACK
         */
        @Override
        protected ByteBuffer allocBuffer() {
            return ByteBuffer.allocate(2 + 2 + blkSize);
        }
        
        /*
         * Session state-shared global variables
         */
        private byte[] data = new byte[512]; // file send/receive data block configuration
        private long rexmtmo; // milliseconds of TFTP protocol packet re-transmit timeout
        private int maxtmos; // max number of timeouts before session is terminated TFTP protocol state
        private int blkSize = 512; // Transfer block size less opCode and ACK
        private int lastAck = 0; // Last ack received from or transmitted to
        // the Tftp client
        private int dataBlockSize; // the number of bytes last read from
        private int nTimeouts; // count timeouts until session timeout
        
        /**
         * Session State class. Each session action method returns the next/new state or null if no
         * transition. Default input handlers are define here. All but sessionTimeuot terminates the
         * session with an error. SessionTimeout terminates gracefully.
         */
        private abstract class SessionState extends AbstractStateMachine.State {
            @Override
            public String toString() {
                return "Tftp" + getRemoteSocketAddr() + "[" + stateName() + "]";
            }
            
            /**
             * Compute the next state given input <i>o</o>. We know that the AbstractUDPSession will receive
             * only 3 kinds of input:
             * <ul><li>An incoming UDP datagran in a ByteBuffer
             * <li>An <i>AsyncEvent</i> object posted from within the state machine itself
             * <li>IOException
             * </ul>
             * The nasty type casting from Object to the correct specific class is done here only.
             */
            protected AbstractStateMachine.State input(Object o) {
                if (o instanceof ByteBuffer)
                    return packet((ByteBuffer)o);
                if (o instanceof AsyncEvent)
                    return ((AsyncEvent)o).fire((SessionState)state);
                if (o instanceof IOException)
                    return ioError((IOException)o);
                log.severe("Unexpected input object class" + o.getClass().getName());
                sendError(errUndef, "Internal server error");
                doneState.setExitStatus(clErr);
                return doneState;
            }
            
            public SessionState packet(ByteBuffer buf) {
                log.severe(this + ": Unexpected data packet received");
                sendError(errUndef, "Unexpected data packet");
                doneState.setExitStatus(clErr);
                return doneState;
            }
            
            public SessionState sessionTimeout() {
                log.info(this + ": Session timeout");
                sendError(errUndef, "Too many timeouts, giving up");
                doneState.setExitStatus(clTmo);
                return doneState;
            }
            
            public SessionState oAckTimeout() {
                log.severe(this + ": Unexpected OAck timeout, giving up");
                sendError(errUndef, "Internal server error");
                doneState.setExitStatus(clErr);
                return doneState;
            }
            
            public SessionState receiveTimeout() {
                log.severe(this + ": Unexpected receive timeout, giving up");
                sendError(errUndef, "Internal server error");
                doneState.setExitStatus(clErr);
                return doneState;
            }
            
            public SessionState transmitTimeOut() {
                log.severe(this + ": Unexpected transmit timeout, giving up");
                sendError(errUndef, "Internal server error");
                doneState.setExitStatus(clErr);
                return doneState;
            }
            
            public SessionState ioError(IOException e) {
                log.info(this + ": " + e);
                doneState.setExitStatus(clErr);
                return doneState;
            }
            
            public SessionState fileOpenWrite(OutputStream f) {
                log.severe(this + ": Illegal state, cannot open file for writing");
                sendError(errUndef, "Internal server error");
                doneState.setExitStatus(clErr);
                return doneState;
            }
            
            public SessionState fileOpenRead(InputStream f) {
                log.severe(this + ": Illegal state, cannot open file for readnig");
                sendError(errUndef, "Internal server error");
                doneState.setExitStatus(clErr);
                return doneState;
            }
            
            public SessionState fileOpenError(short perr, String errmsg) {
                log.severe(this + ": Internal program errror");
                sendError(errUndef, "Internal server error");
                doneState.setExitStatus(clErr);
                return doneState;
            }
        }
        
        /**
         * TFTP Protocol initial state. This packet can only be a
         * Read Request or a Write Request.
         */
        private class InitialState extends SessionState {
            public String stateName() {
                return "Initial";
            }
            
            public SessionState packet(ByteBuffer buf) {
                clearTimeOut();
                int opCode = 0;
                try {
                    // Handle illegal packets without an opCode
                    if (buf.remaining() < 2) {
                        log.info(this + ": Short packet (" + buf.remaining() + " bytes) closing connection");
                        sendError(errIllegalOp, "Illegal packet too short");
                        doneState.setExitStatus(clErr);
                        return doneState;
                    }
                    // Decode packet and execute instructions
                    opCode = buf.getShort();
                    switch (opCode) {
                        case 1: // READ <filename> <mode> ( <optionName>
                            // <optionValue> )...
                        case 2: // WRITE <filename> <mode> ( <optionName>
                            // <optionValue> )...
                            String fileName = unpackASCIIZ(buf);
                            String mode = unpackASCIIZ(buf);
                            log.info(this + ": " + ((opCode == 1) ? "READ" : "WRITE") + " filename=" + fileName + " mode="
                                    + mode);
                            while (buf.remaining() > 0) {
                                String optName = unpackASCIIZ(buf);
                                String optVal = unpackASCIIZ(buf);
                                if (optName.equalsIgnoreCase("blksize") && (blkSize == 512)) {
                                    try {
                                        blkSize = Integer.parseInt(optVal);
                                        if ((blkSize < 8) || (blkSize > 65464)) {
                                            log.info(this + ": Option blksize " + blkSize + " ignored; using 512");
                                            blkSize = 512;
                                        }
                                        log.info(this + ": Using block size " + blkSize);
                                    } catch (NumberFormatException e) {
                                        log.info(this + ": Illegal block size " + optVal + " ignored; using 512");
                                        blkSize = 512;
                                    }
                                }
                            }
                            lastAck = 0; // this is probably needed TODO...
                            if (blkSize != 512) {
                                sendOAck();
                                data = new byte[blkSize];
                            }
                            // The asynchronous nature of the openRead() --> ... --> openRead_cb() is does NOT represent
                            // a race condition because all calls into the state machine are serialized. Thus, even if
                            // the call to openRead_cb occurs BEFORE the return, and thus before the state change to
                            // waitFileOpenState, the return WILL be executed before the openRead_cb event is processed.
                            // see AbstractUDPSession.WorkerThread
                            if (opCode == 1)
                                openRead(TFTPSession.this, fileName);
                            else
                                openWrite(TFTPSession.this, fileName);
                            waitFileOpenState.setMode(mode);
                            return waitFileOpenState;
                        default:
                            log.info(this + ": Illegal opcode" + opCode + " terminate session");
                            sendError(errIllegalOp, "Illegal opcode " + opCode);
                            doneState.setExitStatus(clErr);
                            return doneState;
                    }
                } catch (Throwable e) {
                    log.severe(this + ": Exception " + e + " terminate session");
                    e.printStackTrace(System.err);
                    sendError(errUndef, "Internal server error " + e.getMessage());
                    doneState.setExitStatus(clErr);
                    return doneState;
                }
            }
        }
        
        /**
         * State where we wait for a call to openFileRead_cb or openFileWrite_cb
         */
        private class WaitFileOpenState extends SessionState {
            private String mode;
            
            public String stateName() {
                return "WaitFileOpen";
            }
            
            public void setMode(String mode) {
                this.mode=mode;
            }
            
            public SessionState oAckTimeout() {
                log.finer(this + ": Retransmit OACK");
                sendOAck();
                return null;
            }
            
            public SessionState fileOpenWrite(OutputStream f) {
                log.finer(this + ": Start file receive");
                if (mode.equalsIgnoreCase("netascii"))
                    receiveState.setFile(new NetAsciiFilterOutputStream(f));
                else
                    receiveState.setFile(f);
                return receiveState;
            }
            
            public SessionState fileOpenRead(InputStream f) {
                log.finer(this + ": Start file transmit");
                if (mode.equalsIgnoreCase("netascii"))
                    transferState.setFile(new NetAsciiFilterInputStream(f));
                else
                    transferState.setFile(f);
                return transferState;
            }
            
            public SessionState fileOpenError(short perr, String errmsg) {
                log.finer(this + ": " + errmsg);
                sendError(perr, errmsg);
                doneState.setExitStatus(clErr);
                return doneState;
            }
        }
        
        /**
         * TFTP Protocol state incoming data packet.
         */
        private class ReceiveState extends SessionState {
            private OutputStream rcvFile;
            public String stateName() {
                return "Receive";
            }
            
            public void setFile(OutputStream f) {
                rcvFile = f;
            }
            
            protected void enter() {
                if (lastAck != -1)
                    sendAck((short)0); // send ACK for block number 0 to start transfer
            }
            
            public SessionState receiveTimeout() {
                log.finer(this + ": Timeout retransmit ACK " + ((int) lastAck));
                sendAck((short)lastAck);
                return null;
            }
            
            public SessionState packet(ByteBuffer buf) {
                clearTimeOut();
                int opCode = 0;
                try {
                    // Handle illegal packets without an opCode
                    if (buf.remaining() < 2) {
                        log.info(this + ": Short packet (" + buf.remaining() + " bytes) ignoring");
                        return null;
                    }
                    opCode = buf.getShort();
                    switch (opCode) {
                        case 3: // DATA <packetNum> <blkSize byte data>
                            if (buf.remaining() < 2) {
                                log.info(this + ": Too short DATA packet ignored");
                                sendAck((short)lastAck);
                                return null;
                            }
                            int packetNumber = 0xFFFF & (int) buf.getShort();
                            log.finer(this + ": DATA packet=" + packetNumber + " lastAck = " + lastAck
                                    + " dataBlockSize is " + buf.remaining());
                            if (packetNumber < lastAck) // old data packet, discard
                                log.finest(this + ": Old data packet " + packetNumber + " discarded");
                            else if (packetNumber == lastAck + 1) { // data for next packet
                                SessionState newState = null;
                                sendAck((short)packetNumber);
                                log.finest(this + ": ACK packet " + ((int) packetNumber));
                                if (buf.remaining() < blkSize) { // last block,
                                    log.info(this + ": WRITE request done; lingering");
                                    newState = lingeringState;
                                }
                                receiveData(rcvFile, buf);
                                return newState;
                            } else if (packetNumber == lastAck) { // client missed an ACK
                                log.finer(this + ": Client retransmitted data packet " + lastAck);
                                sendAck((short)packetNumber); // retransmit the ACK
                            } else { // out-of-sync ACK packet
                                log.info(this + ": Out-of-sync-packet received " + packetNumber + " ending connection");
                                sendError(errIllegalOp, "Out-of-sync-data-packet " + packetNumber + ". Protocol violation");
                                doneState.setExitStatus(clErr);
                                return doneState;
                            }
                            return null;
                        case 5: // ERROR
                            String errnum;
                            if (buf.remaining() < 2)
                                errnum = "unknown";
                            else
                                errnum = "" + buf.getShort();
                            log.info(this + ": ERROR received. errnum=" + errnum + " message: " + unpackASCIIZ(buf));
                            doneState.setExitStatus(clErr);
                            return doneState;
                        default:
                            log.info(this + ": Invalid opCode" + opCode + " ignore");
                            sendError(errIllegalOp, "Invalid opcode: " + opCode);
                            doneState.setExitStatus(clErr);
                            return doneState;
                    }
                } catch (Throwable e) {
                    log.severe(this + ": Exception " + e + " terminate session");
                    e.printStackTrace(System.err);
                    sendError(errUndef, "Internal server error " + e.getMessage());
                    doneState.setExitStatus(clErr);
                    return doneState;
                }
            }
            
            protected void leave() {
                try {
                    rcvFile.close();
                } catch (IOException e) {
                    log.warning(this + ": Caught IOException closing file: " + e);
                    e.printStackTrace(System.err);
                }
            }
        }
        
        /**
         * TFTP Protocol handle incoming ACK packet.
         */
        private class TransferState extends SessionState {
            private InputStream sndFile;
            public String stateName() {
                return "Transfer";
            }
            
            public SessionState transmitTimeOut() {
                log.finer(this + ": Timeout Retransmit DATA " + ((int) lastAck + (int) 1));
                sendDataPacket((short) (lastAck + 1), data, dataBlockSize);
                return null;
            }
            
            public void setFile(InputStream f) {
                sndFile = f;
            }
            
            protected void enter() {
                if (lastAck != -1)
                    sendData(sndFile); // Send data block 0 to start transfer
            }
            
            public SessionState packet(ByteBuffer buf) {
                int opCode;
                clearTimeOut();
                try {
                    // Handle illegal packets without an opCode
                    if (buf.remaining() < 2) {
                        log.info(this + ": Short packet (" + buf.remaining() + " bytes) ignoring");
                        return null;
                    }
                    opCode = buf.getShort();
                    switch (opCode) {
                        case 4: // ACK <acknowledge packet number>
                            if (buf.remaining() < 2) {
                                log.info(this + ": ACK packet without ACK number, stop session");
                                sendError(errIllegalOp, "Protocol violation: ACK packet with no ACK number");
                                doneState.setExitStatus(clErr);
                                return doneState;
                            }
                            int ackPacketNumber = 0xFFFF & (int) buf.getShort();
                            log.finest(this + ": received ack " + ackPacketNumber + " lastAck = " + lastAck
                                    + " dataBlockSize is " + dataBlockSize);
                            if (ackPacketNumber < lastAck) // Discard old ACK
                                log.finest(this + ": Old ack " + ackPacketNumber + " will be discarded");
                            else if (ackPacketNumber == lastAck + 1) { // Excpected-ACK
                                lastAck = ackPacketNumber;
                                if (lastAck > 0 && dataBlockSize < blkSize) {
                                    // ack for the last block, terminate session
                                    log.info(this + ": READ request done");
                                    doneState.setExitStatus(clOK);
                                    return doneState;
                                } else { // read and send the next block
                                    log.finest(this + ": Transmit packet " + ((int) 1 + (int) lastAck));
                                    sendData(sndFile);
                                }
                            } else if (ackPacketNumber == lastAck) {
                                log.finer(this + ": Client request retransmit of packet " + ((int) lastAck + (int) 1));
                                sendDataPacket((short) (lastAck + 1), data, dataBlockSize);
                            } else { // out-of-sync ack packet
                                log.info(this + ": Out-of-sync-packet received" + ackPacketNumber + " ending connection");
                                sendError(errIllegalOp, "Out-of-sync-ack-packet; protocol violation");
                                doneState.setExitStatus(clErr);
                                return doneState;
                            }
                            return null;
                        case 5: // ERROR
                            String errnum;
                            if (buf.remaining() < 2)
                                errnum = "unknown";
                            else
                                errnum = "" + buf.getShort();
                            log.info(this + ": ERROR received. errnum=" + errnum + " message: " + unpackASCIIZ(buf));
                            doneState.setExitStatus(clErr);
                            return doneState;
                        default:
                            log.info(this + ": Invalid opCode" + opCode + " ignore");
                            sendError(errIllegalOp, "Invalid opcode: " + opCode);
                            doneState.setExitStatus(clErr);
                            return doneState;
                    }
                } catch (Throwable e) {
                    log.severe(this + ": Exception " + e + " terminate session");
                    e.printStackTrace(System.err);
                    sendError(errUndef, "Internal server error " + e.getMessage());
                    doneState.setExitStatus(clErr);
                    return doneState;
                } finally {
                }
            }
            
            protected void leave() {
                try {
                    sndFile.close();
                } catch (IOException e) {
                    log.warning(this + ": Caught IOException closing file: " + e);
                    e.printStackTrace(System.err);
                }
            }
        }
        
        /**
         * Closing state we linger, retransmitting the last Ack to our client if requested
         */
        private class LingeringState extends ReceiveState {
            public String stateName() {
                return "Lingering";
            }
            
            public SessionState packet(ByteBuffer buf) {
                sendAck((short)lastAck);
                return null;
            }
            
            public SessionState ioError(IOException e) {
                log.finer(this + ": Session interrupted " + e);
                doneState.setExitStatus(clErr);
                return doneState;
            }
            
            public SessionState receiveTimeout() {
                log.finest("Lingering...");
                setTimeOut(receiveTimeoutEvent);
                return null;
            }
            
            public SessionState sessionTimeout() {
                log.info(this + ": Session complete");
                doneState.setExitStatus(clOK);
                return doneState;
            }
        }
        
        /**
         * The done state should never receive any events, but logs and ends the session
         */
        private class DoneState extends SessionState {
            private int status;
            public String stateName() {
                return "Done";
            }
            
            public void setExitStatus(int status) {
                this.status=status;
            }
            
            public void enter() {
                log.finer(this + ": Cleaning up");
                clearTimeOut();
                TFTPSession.super.cleanupClient();
            }
            
            private SessionState doit(String s) {
                log.severe(this + " Unexpected " + s);
                return null;
            }
            
            public SessionState packet(ByteBuffer buf) {
                return doit("Packet received");
            }
            
            public SessionState sessionTimeout() {
                return doit("Session timeout received");
            }
            
            public SessionState oAckTimeout() {
                return doit("OACK timeout received in");
            }
            
            public SessionState receiveTimeout() {
                return doit("Receive-timeout received");
            }
            
            public SessionState transmitTimeOut() {
                return doit("Transmit timeout received");
            }
            
            public SessionState ioError(IOException e) {
                return doit("IOError received in");
            }
            
            public SessionState fileOpenWrite(OutputStream f) {
                return doit("File open write received");
            }
            
            public SessionState fileOpenRead(InputStream f) {
                return doit("File open read received");
            }
        }
        
        private class ShutdownState extends DoneState {
            public String stateName() {
                return "Shutdown";
            }
            public void enter() {
                log.finer(this + ": Shutting down");
                sendError(errUndef, "Server shutting down");
                super.enter();
            }
        }
        
        /**
         * Set a timeout&mdash;synchronized to avoid race condition where
         *
         * @param _event
         *            the event to fire.
         */
        private synchronized void setTimeOut(final AsyncEvent _event) {
            timeoutHandler = new TimerTask() {
                public void run() {
                    if (incTimeouts() == maxtmos)
                        postInput(sessionTimeoutEvent);
                    else
                        postInput(_event);
                }
            };
            timer.schedule(timeoutHandler, rexmtmo);
        }
        
        /**
         * Increase the number of timeouts counted so far and return the new count
         */
        private synchronized int incTimeouts() {
            return ++nTimeouts;
        }
        
        /**
         * Clear the timeout
         */
        private synchronized void clearTimeOut() {
            nTimeouts = 0;
            if (timeoutHandler != null) {
                timeoutHandler.cancel();
                timeoutHandler = null;
            }
        }
        
        /*
         * Low-level Routines to parse and pack TFTP protocol datagrams
         */
        /**
         * Decodes the next part of the datagram as a nul terminated ASCII string
         *
         * @param buf
         *            The buffer holding the datagram read from the Tftp client
         * @return the String object created from the contents of buf
         */
        private String unpackASCIIZ(ByteBuffer buf) {
            String s;
            byte b;
            for (s = ""; (buf.remaining() > 0) && (b = buf.get()) != 0; s += (char) b)
                ;
            return s;
        }
        
        /**
         * Fills the buf object with an TFTP ERROR datagram and sends it
         *
         * @param errcode
         *            the TFTP Error code to send.
         * @param msg
         *            A message to accompany the error code in the datagram
         */
        private void sendError(short errcode, String msg) {
            ByteBuffer buf = allocBuffer();
            buf.clear();
            buf.putShort((short) 5); // ERROR errcode <msg>
            buf.putShort(errcode);
            buf.put(msg.getBytes());
            buf.put((byte) 0);
            buf.flip();
            try {
                connection.write(buf);
            } catch (Exception e) {
                log.warning(toString() + ": Exception ignored sending error packet. " + e.getMessage());
                e.printStackTrace(System.err);
            }
        }
        
        /**
         * Send an OACK (currently only with block size option).
         */
        private void sendOAck() {
            ByteBuffer buf = allocBuffer();
            buf.clear();
            buf.putShort((short) 6); // OACK <optionZ> <valueZ>
            buf.put("blksize".getBytes());
            buf.put((byte) 0);
            buf.put(("" + blkSize).getBytes());
            buf.put((byte) 0);
            buf.flip();
            try {
                connection.write(buf);
            } catch (IOException e) {
                log.warning(this + ": Exception caught sending OAck packet. "+ e);
                e.printStackTrace();
            }
            lastAck = -1;
            setTimeOut(oAckTimeoutEvent);
        }
        
        /**
         * Send an ACK for the last block received and set a timeout.
         */
        private void sendAck(short ackNum) {
            ByteBuffer buf = allocBuffer();
            buf.clear();
            buf.putShort((short) 4); // ACK <ackNum>
            buf.putShort((short) ackNum);
            buf.flip();
            try {
                connection.write(buf);
            } catch (IOException e) {
                log.warning(this + ": Exception caught sending Ack: " + e);
                e.printStackTrace();
            }
            lastAck = ackNum;
            setTimeOut(receiveTimeoutEvent);
        }
        
        /**
         * Sends a data packet to the client. Build the buf object as a TFTP protocol datagram and
         * sends the data. A timer is set to timeout after rexmtmo milliseconds.
         *
         * @param ackNum
         *            The TFTP block number of this data packet
         * @param data
         *            a byte array blkSize bytes
         * @param dbs
         *            the number of bytes in data
         */
        private void sendDataPacket(short ackNum, byte[] data, int dbs) {
            ByteBuffer buf = allocBuffer();
            buf.clear();
            buf.putShort((short) 3); // DATA ACK <data>
            buf.putShort(ackNum);
            buf.put(data, 0, dbs);
            buf.flip();
            try {
                connection.write(buf);
            } catch (IOException e) {
                log.warning(this + ": Caught exception sending data packet: " + e);
                e.printStackTrace();
            }
            setTimeOut(transmitTimeoutEvent);
        }
        
        /*
         * Medium level routines.
         */
        /**
         * Callback to call when there is an error opening the file or data stream.
         *
         * @param perr
         *            TFTP Protocol error code to return to client.
         * @param errmsg
         *            Error message to include in Tftp error packet
         * @see TftpListener#openRead
         */
        public void openFileRead_cb(short perr, String errmsg) {
            errorOpenFileEvent.setArgs(perr, errmsg);
            postInput(errorOpenFileEvent);
        }
        
        /**
         * Callback or Call-in to call from openread() when file has been opened. The application
         * should arange for this method to be called, after the openRead() abstract method
         * succeeds. If the file should be transferred using netascii transfer mode it is wrapped in
         * a NetAsciiFilterInputStream object.
         *
         * @param f
         *            The InputStream representing the data to be sent to the Tftp client. If f is
         *            null it means the application could not open the file for some reason, and an
         *            error is returned to the Tftp client.
         * @see TftpListener#openRead
         */
        public void openFileRead_cb(InputStream f) {
            if (f == null)
                throw new IllegalArgumentException("Cannot send null file");
            fileOpenReadEvent.setInputStream(f);
            postInput(fileOpenReadEvent);
        }
        
        /**
         * Callback called from openwrite() when there is an error opening the file or data stream.
         *
         * @param perr
         *            Protocol error to return to client.
         * @param errmsg
         *            Error message to include in Tftp error packet
         * @see TftpListener#openRead
         */
        public void openFileWrite_cb(short perr, String errmsg) {
            errorOpenFileEvent.setArgs(perr, errmsg);
            postInput(errorOpenFileEvent);
            return;
        }
        
        /**
         * Callback to call from openwrite() when file has been opened for writing. The application
         * should arange for this method to be called after the tftpEventListener.openRead()
         * succeeds. If the file should be transferred using netascii transfer mode it is wrapped in
         * a NetAsciiFilterOutputStream object.
         * <p>
         *
         * @param f
         *            The OutputStream representing the data sink to receive data from the Tftp
         *            client. If f is null it means the application could not open the file for some
         *            reason, and an error is returned to the Tftp client.
         * @see TftpListener#openWrite
         */
        public void openFileWrite_cb(OutputStream f) {
            if (f == null)
                throw new IllegalArgumentException("Cannot receive to null file");
            fileOpenWriteEvent.setOutputStream(f);
            postInput(fileOpenWriteEvent);
        }
        
        /**
         * Register an event handler to be called when the session terminates
         *
         * @param sessionDone
         *            the event handler implementing the SessionDone interface
         */
        public void registerSessionDoneHandler(final SessionDone sessionDone) {
            doneState.registerStateChangeListener(new AbstractStateMachine.StateChangeListener() {
                public void newState(AbstractStateMachine m, AbstractStateMachine.State s) {
                    sessionDone.sessionDone((TFTPSession) m, ((DoneState)s).status);
                }
            });
        }
        
        /**
         * Read and send the next block of data from the InputStream to the TFTP client.
         */
        private void sendData(InputStream sndFile) {
            try {
                if ((dataBlockSize = sndFile.read(data)) < 0)
                    dataBlockSize = 0;
            } catch (IOException e) {
                log.warning(this + ": Caught IOException reading data block " + e);
                e.printStackTrace(System.err);
            }
            sendDataPacket((short) (lastAck + 1), data, dataBlockSize);
        }
        
        /**
         * Write a data block to the output file from buf.
         */
        private void receiveData(OutputStream rcvFile, ByteBuffer buf) throws IOException {
            int n;
            buf.get(data, 0, (n = buf.remaining()));
            rcvFile.write(data, 0, n);
        }
        
        /**
         * AsyncEvent and its derived classes delegate the responsibility of handling the event to
         * specific methods in SessionState
         */
        private abstract class AsyncEvent {
            public abstract SessionState fire(SessionState state);
        }
        
        private class FileOpenReadEvent extends AsyncEvent {
            private InputStream f;
            public void setInputStream(InputStream f) {
                this.f=f;
            }
            public SessionState fire(SessionState state) {
                return state.fileOpenRead(f);
            }
        }
        
        private class FileOpenWriteEvent extends AsyncEvent {
            private OutputStream f;
            public void setOutputStream(OutputStream f) {
                this.f=f;
            }
            public SessionState fire(SessionState state) {
                return state.fileOpenWrite(f);
            }
        }
        
        private class TransmitTimeoutEvent extends AsyncEvent {
            public SessionState fire(SessionState state) {
                return state.transmitTimeOut();
            }
        }
        
        private class ReceiveTimeoutEvent extends AsyncEvent {
            public SessionState fire(SessionState state) {
                return state.receiveTimeout();
            }
        }
        
        private class OAckTimeoutEvent extends AsyncEvent {
            public SessionState fire(SessionState state) {
                return state.oAckTimeout();
            }
        }
        
        private class SessionTimeoutEvent extends AsyncEvent {
            public SessionState fire(SessionState state) {
                return state.sessionTimeout();
            }
        }
        
        private class ErrorOpenFileEvent extends AsyncEvent {
            private short perr;
            private String errmsg;
            
            public void setArgs(short perr, String errmsg) {
                this.perr = perr;
                this.errmsg = errmsg;
            }
            
            public SessionState fire(SessionState state) {
                return state.fileOpenError(perr, errmsg);
            }
        }
        
        /*
         * Constant Asynchronous events
         */
        private final FileOpenReadEvent fileOpenReadEvent = new FileOpenReadEvent();
        private final FileOpenWriteEvent fileOpenWriteEvent = new FileOpenWriteEvent();
        private final ErrorOpenFileEvent errorOpenFileEvent = new ErrorOpenFileEvent();
        private final TransmitTimeoutEvent transmitTimeoutEvent = new TransmitTimeoutEvent();
        private final ReceiveTimeoutEvent receiveTimeoutEvent = new ReceiveTimeoutEvent();
        private final OAckTimeoutEvent oAckTimeoutEvent = new OAckTimeoutEvent();
        private final SessionTimeoutEvent sessionTimeoutEvent = new SessionTimeoutEvent();
        /*
         * Constant Session states
         */
        private final InitialState initialState = new InitialState();
        private final WaitFileOpenState waitFileOpenState = new WaitFileOpenState();
        private final TransferState transferState = new TransferState();
        private final ReceiveState receiveState = new ReceiveState();
        private final LingeringState lingeringState = new LingeringState();
        private final DoneState doneState = new DoneState();
        private final ShutdownState shutdownState = new ShutdownState();
    }
    
    // Session-done / File-Close reasons passed with SessionDone object.
    public static final int clOK = 0; // everything is OK
    public static final int clTmo = 1; // Session timed out
    public static final int clErr = 2; // There was an error
    // TFTP protocol constants
    public static final short errOK = -1; // not an error
    public static final short errUndef = 0; // undefined error
    public static final short errNotFound = 1; // File not found.
    public static final short errAccess = 2; // Access violation
    public static final short errDiskFull = 3; // Disk full or allocation exceeded
    public static final short errIllegalOp = 4; // Illegal TFTP operation.
    public static final short errUnknownID = 5; // Unknown transfer ID.
    public static final short errFileExists = 6; // File already exists.
    public static final short errNoUser = 7; // No such user.
}
