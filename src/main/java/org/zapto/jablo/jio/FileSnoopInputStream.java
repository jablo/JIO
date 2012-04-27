/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.zapto.jablo.jio;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.logging.Logger;

/**
 * A Input stream file class that dumps its input to a file in a specified directory
 * while it passes through.
 * @author Jacob Lorensen, YouSee, 2008
 */
public class FileSnoopInputStream extends PipedInputStream implements Runnable {
    private static final Logger log = Logger.getLogger(FileSnoopInputStream.class.getName());
    private final InputStream source;
    private final PipedOutputStream pipe;
    private final OutputStream os;
    private Thread t;

    public FileSnoopInputStream(InputStream source, File logFile) throws FileNotFoundException, IOException {
        os = new FileOutputStream(logFile);
        this.source = source;
        pipe = new PipedOutputStream();
        connect(pipe);
        t = new Thread(this, FileSnoopInputStream.class.getSimpleName()+"-"+source);
        t.setDaemon(true);
        t.start();
    }

    /**
     * Read from input side, write to trace file and to pipe. At end close trace file and pipe, passing
     * the close signal on to our reading end.
     */
    public void run() {
        try {
            byte[] buf = new byte[4096]; // any reasonably sized buffer will do,I guess.
            int n;
            while ((n = source.read(buf)) > 0) {
                os.write(buf, 0, n);
                pipe.write(buf, 0, n);
            }
        } catch (IOException ex) {
            log.warning("Error " + ex);
        } finally {
            try {
                os.close();
            } catch (IOException ex) {
                log.info(ex.getMessage());
            }
            try {
                pipe.close();
            } catch (IOException ex) {
                log.info(ex.getMessage());
            }
            try {
                source.close();
            } catch (IOException ex) {
                log.info(ex.getMessage());
            }
        }
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            try {
                source.close();
            } finally {
                System.out.println("Waiting for thread: " + t);
                try {
                    t.join();
                } catch (InterruptedException e) {
                    System.out.println("Interrupted waiting for thread " + t + " to terminate");
                    return;
                }
                System.out.println("Thread " + t + " terminated ok");
            }
        }
    }
}
