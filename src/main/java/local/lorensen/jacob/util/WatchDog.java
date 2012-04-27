/*
 * $Id: WatchDog.java,v 1.1.1.1 2008-09-04 21:43:12 jablo Exp $
 * $Name: not supported by cvs2svn $
 *
 * Jacob Lorensen, TDC KabelTV, 2006.
 */
package local.lorensen.jacob.util;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * WatchDog class implements a checklist/actionlist as a TimerTask.
 * 
 * Schedule it periodically on a Timer object, e.g.
 * <code><pre>
 *   ClassThasImplementsGuardInterface c = new ClassThasImplementsGuardInterface(...);
 *   WatchDog w = new WatchDog();
 *   w.add(c);
 *   Timer t = new Timer(...);
 *   t.scheduleAtFixedRate(w, ...);
 * </pre></code>
 */
public class WatchDog extends TimerTask {
    private LinkedList conditions;
    private static final Logger log = Logger.getLogger(WatchDog.class.getName());

    /**
     * Create an instance of WatchDog.
     */
    public WatchDog() {
	conditions = new LinkedList();
    }

    /**
     * Checks each condition in turn, logs, and tries if necessary to
     * rectify the condition.
     */
    public void run() {
	log.finest("WatchDog run");
        if (conditions.size()==0) return;
	for (Iterator it = conditions.iterator(); it.hasNext();) {
	    Guard g = (Guard) it.next();
            try  {
                log.log(Level.FINEST, "Checking condition {0}", g.guardText());
	        if (g.guardCheck()) {
	            log.log(Level.FINEST, "Condition ok {0}", g.guardText());
	            continue;
	        }
	        log.log(Level.FINE, "Condition failed {0}", g.guardText());
	        g.guardRepair();
	    } catch (Throwable e) {
	        log.log(Level.WARNING, "Exception executing guard {0}", e);
	    }
	}
    }

    /**
     * Adds a new condition to check.
     * 
     * @param guard
     *                An object implementing the WatchDog.Guard interface.
     */
    public void add(Guard guard) {
	if (guard==null) throw new IllegalArgumentException("Guard cannot be null");
        try { 
            log.finest("Adding guard " + guard.guardText());
        } catch (Throwable e) {
            throw new IllegalArgumentException("Illegal object Guard, throws eception "+e);
        }
        conditions.add(guard);
    }
    
    /**
     * Removes a condition from the watchdog list.
     * 
     * @param guard
     *            An object implementing the WatchDog.Guard interface to be
     *            removed from the list of currently moitored objects.
     */
    public void remove(Guard guard) {
        if (guard == null)
            throw new IllegalArgumentException("Guard cannot be null");
        try {
            log.finest("Removing Guard " + guard.guardText());
        } catch (Throwable e) {
            throw new IllegalArgumentException("Illegal object Guard, throws exception " + e);
        }
        conditions.remove(guard);
    }

    /**
     * The Guard Interface describes the object that can be used to check
     * and repair a condition.
     */
    public interface Guard {
	/**
	 * A string describing the condition.
	 * 
	 * @return The description of the condition checked.
	 */
	String guardText();

	/**
	 * Checks a certain condition and returns true or false depending on its
	 * state.
	 * 
	 * @return True if condition is ok, False otherwise.
	 */
	boolean guardCheck();

	/**
	 * Tries to repair a failed condition. registered with WatchDog object.
	 */
	void guardRepair();
    }
}
