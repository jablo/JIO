/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package local.lorensen.jacob.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A simple naïve implementation of a resource pool.
 * @author Jacob Lorensen, YouSee, 2011-09-22
 */
public class SimpleResourcePool<T extends Closeable> {
    private final static Logger log = Logger.getLogger(SimpleResourcePool.class.getName());
    private final List<T> pool;
    private final ResourceCreator<T> factory;

    public SimpleResourcePool(ResourceCreator<T> factory) {
        this(factory, 0);
    }

    public SimpleResourcePool(ResourceCreator<T> factory, int prePopulate) {
        this.factory = factory;
        this.pool = new ArrayList();
        if (prePopulate < 0)
            throw new IllegalArgumentException("Cannot have negative number of rsources in pool");
        while (prePopulate-- > 0)
            pool.add(factory.open());
    }

    public interface ResourceCreator<TT extends Closeable> {
        TT open();
    }
    private int n = 0;

    public synchronized T getResource() {
        if (pool.isEmpty()) {
            n++;
            log.log(Level.FINER, "Creating resource {0}", n);
            pool.add(factory.open());
        }
        log.finer("reusing resource");
        return pool.remove(0);
    }

    public synchronized void putResource(T r) {
        pool.add(r);
    }

    public void close() {
        for (T r : pool)
            try {
                r.close();
            } catch (IOException ex) {
                Logger.getLogger(SimpleResourcePool.class.getName()).log(Level.SEVERE, null, ex);
            }
    }
}
