/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.zapto.jablo.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * A number of generic list iteration utilities inspired from functional programming languages:
 * <ul><li><code>map</code>&mdash;applies a function to every element in a list
 * <li><code>filter</code>&mdash;creates a list of elements that fulfill a certain preedicate
 * <li><code>fold</code>&mdash;folds a list with a function and a start value.
 * </ul>
 * @author Jacob Lorensen, YouSee, November 2008
 */
public class CollectionUtils {
    /**
     * A function that can be used by the map utility.
     * @param <A>
     * @param <B>
     * @see #map(ktv.samiscollector.impl.util.CollectionUtils.Mapper, java.util.List, java.util.List) 
     * @see #map(ktv.samiscollector.impl.util.CollectionUtils.Mapper, java.util.List) 
     */
    public interface Mapper<A, B> {
        public B f(A a);
    }

    /**
     * A function that can be used by the map utility iterating through a list without building a result list.
     * @param <A>
     * @param <B>
     * @see #map(ktv.samiscollector.impl.util.CollectionUtils.MapperProc, java.util.List) 
     */
    public interface MapperProc<A> {
        public void f(A a);
    }

    /**
     * A function that can be used by the fold utility
     * @param <A>
     * @param <B>
     * @see #fold(ktv.samiscollector.impl.util.CollectionUtils.Folder, java.lang.Iterable, java.lang.Object) 
     */
    public interface Folder<A, B> {
        public B f(B a1, A a2);
    }

    /**
     * A function that can be used by the filter utility
     * @param <A>
     * @see #filter(ktv.samiscollector.impl.util.CollectionUtils.Filter, java.util.List) 
     * @see #filter(ktv.samiscollector.impl.util.CollectionUtils.Filter, java.util.List, java.util.List) 
     */
    public interface Filter<A> extends Mapper<A, Boolean> {
    }
    /**
     * A clone mapper, i.e. it will invoke clone and return the result of on every object. Can be used
     * to make a deeper clone of a list of cloneable objects.
     * <code><pre>
     *   List<Long> l1 = &hellip;
     *   List<Long> l2 = CollectionUtils.map(CollectionUtils.cloneMapper, l1);
     * </pre></code>
     * Now <code>l2</code> is a clone of <code>l1</code>, and the objects in <code>l2</code> are clones of the 
     * objects in <code>l1</code>
     */
    public final static Mapper<Cloneable, Cloneable> cloneMapper = new Mapper<Cloneable, Cloneable>() {
        @Override
        public Cloneable f(Cloneable a) {
            try {
                Method clone = a.getClass().getMethod("clone");
                return (Cloneable) clone.invoke(a);
            } catch (IllegalAccessException ex) {
                throw new RuntimeException(ex);
            } catch (IllegalArgumentException ex) {
                throw new RuntimeException(ex);
            } catch (InvocationTargetException ex) {
                throw new RuntimeException(ex);
            } catch (NoSuchMethodException ex) {
                throw new RuntimeException(ex);
            } catch (SecurityException ex) {
                throw new RuntimeException(ex);
            }
        }
    };
    /**
     * A trivial filter predicate, that returns true for all objects
     */
    public final static Filter<Object> trueFilter = new Filter<Object>() {
        @Override
        public Boolean f(Object a) {
            return true;
        }
    };
    /**
     * The trivial filter predicate that return false for all objects
     */
    public final static Filter<Object> flaseFilter = new Filter<Object>() {
        @Override
        public Boolean f(Object a) {
            return false;
        }
    };

    /**
     * Filters a list with a predicate and produces a kl
     * @param <A>
     * @param filter
     * @param in
     * @param out the result list with elements <code>a</code> from <code>in</code> where <code>filter.f(a) == true</code>
     */
    public static <A> void filter(Filter<A> filter, List<A> in, List<A> out) {
        out.clear();
        for (A a : in)
            if (filter.f(a))
                out.add(a);
    }

    /**
     * Functional form of filter
     * @param <A>
     * @param filter
     * @param in
     * @return a list with elements <code>a</code> from <code>in</code> where <code>filter.f(a) == true</code>
     * @see #filter(ktv.samiscollector.impl.util.CollectionUtils.Filter, java.util.List, java.util.List) 
     */
    public static <A> List<A> filter(Filter<A> filter, List<A> in) {
        try {
            Constructor c = in.getClass().getConstructor();
            List<A> out = (List<A>) c.newInstance();
            filter(filter, in, out);
            return out;
        } catch (NoSuchMethodException ex) {
            throw new RuntimeException(ex);
        } catch (SecurityException ex) {
            throw new RuntimeException(ex);
        } catch (InstantiationException ex) {
            throw new RuntimeException(ex);
        } catch (IllegalAccessException ex) {
            throw new RuntimeException(ex);
        } catch (IllegalArgumentException ex) {
            throw new RuntimeException(ex);
        } catch (InvocationTargetException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static <A, B> B fold(Folder<A, B> folder, Iterable<A> in, B acc) {
        for (A a : in)
            acc = folder.f(acc, a);
        return acc;
    }

    public static <A, B> void map(MapperProc<A> mapper, List<A> in) {
        for (A a : in)
            mapper.f(a);
    }

    public static <A, B> void map(Mapper<A, B> mapper, List<A> in, List<B> out) {
        out.clear();
        for (A a : in)
            out.add(mapper.f(a));
    }

    public static <A, B> List<B> map(Mapper<A, B> mapper, List<A> in) {
        try {
            Constructor c = in.getClass().getConstructor();
            List<B> out = (List<B>) c.newInstance();
            map(mapper, in, out);
            return out;
        } catch (NoSuchMethodException ex) {
            throw new RuntimeException(ex);
        } catch (SecurityException ex) {
            throw new RuntimeException(ex);
        } catch (InstantiationException ex) {
            throw new RuntimeException(ex);
        } catch (IllegalAccessException ex) {
            throw new RuntimeException(ex);
        } catch (IllegalArgumentException ex) {
            throw new RuntimeException(ex);
        } catch (InvocationTargetException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static void main(String[] argv) {
        List<Integer> l1 = new LinkedList();
        l1.add(10);
        l1.add(20);
        l1.add(30);
        List<Long> l2 = map(new Mapper<Integer, Long>() {
            @Override
            public Long f(Integer a) {
                return new Long(a);
            }
        }, l1);
        System.out.println("l1: " + l1);
        System.out.println("l2: " + l2);

        List<Integer> l3 = new ArrayList();
        l3.add(10);
        l3.add(20);
        l3.add(30);
        List<Long> l4 = map(new Mapper<Integer, Long>() {
            @Override
            public Long f(Integer a) {
                return new Long(2 * a);
            }
        }, l3);
        System.out.println("l3: " + l3);
        System.out.println("l4: " + l4);
    }
}
