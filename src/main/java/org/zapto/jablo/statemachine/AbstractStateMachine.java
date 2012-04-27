/*
 * AbstractStateMachine.java
 *
 * Created on 19. maj 2007, 22:01
 *
 * $Id: AbstractStateMachine.java,v 1.1.1.1 2008-09-04 21:43:12 jablo Exp $
 * $Name: not supported by cvs2svn $
 *
 */

package org.zapto.jablo.statemachine;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.logging.Logger;

/**
 * Simple generic state machine using one class per state.
 * @author Jacob Lorensen, 2007.
 */
public abstract class AbstractStateMachine {
    private static Logger log = Logger.getLogger(AbstractStateMachine.class.getName());
    /** The current state of the state machine&mdash;treat as read-only */
    protected State state;
    /** A list of objects interested in ALL state changes */
    private LinkedList stateChangeListeners = null;
    
    /**
     * The interface implemented buy clients that wish to receive information when the stat machine changes state
     */
    public interface StateChangeListener {
        public void newState(AbstractStateMachine m, State s);
    }

    /**
     * A runtime exception that an be thrown from state method classes to signal that
     * the method in question is not allowed in the state.
     * The exception automatically add the name of the satet to the exception message.
     */
    public class IllegalStateException extends RuntimeException {
        public IllegalStateException() {
            super(state.stateName()+" does not allow this operation");
        }
        public IllegalStateException(String msg) {
            super(state.stateName()+" "+msg);
        }
    }
    
    /**
     * Start the state machine by placing it in the initial state
     */
    public final void initialize() {
        changeState(initialState());
    }
    
    /**
     * Register an object that is called each time the state machine changes state
     * @param scl The StateChangeListener object
     */
    public void registerStateChangeListener(StateChangeListener scl) {
        if (stateChangeListeners==null) stateChangeListeners=new LinkedList();
        if (stateChangeListeners.contains(scl)) return;
        stateChangeListeners.add(scl);
    }
    
    /** Register an object that is called each time the state machine changes state to
     * a specific state
     * @param scl the state change listener object
     * @param s the state we are interested in being informed about
     */
    public void registerStateChangeListener(StateChangeListener scl, State s) {
        s.registerStateChangeListener(scl);
    }
    
    /**
     * Provide input to the state machine, possibly moving it to a new state
     */
    public void input(Object o) {
        State newState = state.input(o);
        if (newState==null) return;
        changeState(newState);
    }
    
    /**
     * External forced shutdown of the state machine provides a hook to clean up when shutting down the state machine.
     */
    public final void shutdown() {
        changeState(shutdownState());
    }
    
    private void changeState(State newState) {
        log.fine(state + " --> " + newState);
        if (state!=null) state.leave();
        state = newState;
        state.enter();
        if (stateChangeListeners != null) {
            // publish the new state to all interested global listeners.
            for (Iterator it=stateChangeListeners.iterator(); it.hasNext(); ) {
                StateChangeListener scl = (StateChangeListener)it.next();
                scl.newState(this, newState);
            }
        }
        state.publish();
    }
    
    /**
     * Users of this class must provide an initial state through overriding this function
     */
    protected abstract State initialState();
    
    /**
     * Users of this class must provide a shutDown state&mdash;a special state that is entered
     * when a forced external shutdown of the state machine is done through calling shutdown().
     */
    protected abstract State shutdownState();
    
    /**
     * Base abstract class for state machine states.
     */
    public abstract class State {
        /** A list of objects interested in being notified when we enter this state */
        private LinkedList stateChangeListeners = null;
        @Override
        public String toString() {
            return "[" + stateName() + "]";
        }
        
        /**
         * Register an object that is called each time the state machine enters a specific state
         * @param scl The StateChangeListener object
         */
        public void registerStateChangeListener(StateChangeListener scl) {
            if (stateChangeListeners==null) stateChangeListeners=new LinkedList();
            if (stateChangeListeners.contains(scl)) return;
            stateChangeListeners.add(scl);
        }
        
        private void publish() {
            if (stateChangeListeners != null) {
                // publish the new state to all interested listeners.
                for (Iterator it=stateChangeListeners.iterator(); it.hasNext(); ) {
                    StateChangeListener scl = (StateChangeListener)it.next();
                    scl.newState(AbstractStateMachine.this, this);
                }
            }
        }
        
        /** The state name */
        protected abstract String stateName();
        /** Compute the next state given input <i>o</i> */
        protected abstract State input(Object o);
        /** Routine called when we enter the new state */
        protected void enter() {}
        /** Routine called when we leave the old state */
        protected void leave() {}
    }
}
