/*
 * RotatingFileInputStream.java
 *
 * Created on 25. oktober 2007, 22:27
 *
 * $Id: RotatingFileInputStream.java,v 1.1.1.1 2008-09-04 21:43:12 jablo Exp $
 * $Name: not supported by cvs2svn $
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package local.lorensen.jacob.jio;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

/**
 * This class can be used to monitor and follow live all data changes to a named file in the file system.
 * The named file is periodically polled for changes: If data is appended to the file
 * the data is read from the file and written into the writing end of a PipedOutputStream. All new
 * data appended to the file will thus be available for reading from this object. Further,
 * if the named file's size shrinks the file is re-opened and all data in it read.
 * <p>
 * To use this class, just
 * <pre>
 *   File myLogFile = new File("/var/log/messages");
 *   RotatingFileInputStream myLog = new RotatingFileInputStream(myLogFile);
 *   BufferedReader myLogReader = new BufferedReader(new InputStreamReader(myLog));
 *   Thread t = new Thread(myLog);
 *   String l;
 *   t.start();
 *   while ((l=myLogReader.readLine())!=null)
 *      System.out.println("Read line: "+l);
 * </pre>
 *
 * @author JABLO
 */
public class RotatingFileInputStream extends PipedInputStream implements Runnable {
    private File f;
    private FileInputStream currentSrc;
    private PipedOutputStream pipe;
    private int pollTime;
    private long lastLength;
    private volatile boolean stop;
    
    public RotatingFileInputStream(File f) throws IOException {
        this(f, 200);
    }
    /**
     * Creates a new instance of RotatingFileInputStream
     */
    public RotatingFileInputStream(File f, int pollTime) throws IOException {
        this.f = f; this.pollTime = pollTime;
        currentSrc = new FileInputStream(f);
        lastLength = f.length();
        pipe = new PipedOutputStream(this);
        stop=false;
    }
    
    @Override
    public void close() throws IOException {
        stop=true;
        pipe.close();
        super.close();
    }
    
    /**
     * Watches the file changes and replicates data to the pipe.
     */
    public void run() {
        byte[] buf = new byte[8192];
        for (;!stop;) {
            // Poll the length of the file
            long newLength;
            if (!f.exists() || (newLength=f.length())==lastLength) {
                try {
                    Thread.sleep(pollTime);
                } catch (InterruptedException ex) {
                    // ignore
                }
                continue;
                // Did the file shrink since last time? Then it's probably been renamed/deleted and a new file created. Reopen.
            } else if (newLength < lastLength || currentSrc==null) {
                try {
                    if (currentSrc!=null) currentSrc.close();
                } catch (IOException ex) {
                }
                try {
                    lastLength = 0;
                    currentSrc = new FileInputStream(f);
                } catch (FileNotFoundException ex) {
                    currentSrc = null;
                }
                if (newLength==0)
                    continue;
            }
            // newLength > lastLength
            try {
                int n;
                while ((n=currentSrc.read(buf)) > 0) {
                    lastLength += n;
                    pipe.write(buf, 0, n);
                }
            } catch (IOException ex) {
                // Ignore, close() has been called.
            }
        }
    }
    
    /**
     * Small demo/test program.
     */
    public static void main(String args[]) throws IOException {
        RotatingFileInputStream is = new RotatingFileInputStream(new File(args[0]));
        BufferedReader r = new BufferedReader(new InputStreamReader(is));
        String l;
        Thread t = new Thread(is);
        t.setDaemon(true);
        t.start();
        while ((l = r.readLine()) != null)
            System.out.println(l);
    }
}
