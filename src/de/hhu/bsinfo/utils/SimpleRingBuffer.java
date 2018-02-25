package de.hhu.bsinfo.utils;

import java.util.Arrays;
import java.util.LinkedList;

/**
 * Simple implementation of a ring buffer
 *
 * @author Florian Hucke (florian.hucke@hhu.de) on 22.02.18
 * @projectname dxram-memory
 */
public final class SimpleRingBuffer<T> {
    private final int m_size;
    LinkedList<T> buf = new LinkedList<>();

    /**
     * Constructor
     *
     * @param p_size
     *          Max size of the ring buffer
     */
    public SimpleRingBuffer(int p_size){
        m_size = p_size;
    }

    /**
     * Add a object to the ring buffer
     *
     * @param t Object to add
     */
    public void add(T t){
        if(buf.size() == m_size)
            buf.removeLast();

        buf.addFirst(t);
    }

    @Override
    public String toString() {
        return Arrays.toString(buf.toArray());
    }
}
