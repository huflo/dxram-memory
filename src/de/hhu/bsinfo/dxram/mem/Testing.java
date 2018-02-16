package de.hhu.bsinfo.dxram.mem;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A unit to test all the implementations
 * 
 * @author Florian Hucke (florian.hucke@hhu.de) on 13.02.18
 * @projectname dxram-memory
 */
@SuppressWarnings("unused")
public class Testing {
    private final MemoryManagerComponent m;


    /**
     * Create a testing instance
     */
    public Testing(){
        m = new MemoryManagerComponent();
        m.initComponent();

    }

    /**
     * Destroy the testing instance
     */
    public void destroy(){
        m.shutdownComponent();
    }

    /**
     * Multi-Threaded locking test with n-chunks, with equal chunk size
     *
     * @param operations Operation count
     * @param size Chunk size
     * @param n_threads Active threads at the same time
     * @param n_chunks Allocated chunks
     * @param writeProbability Probability of a write access
     * @throws InterruptedException Termination can throw this exception
     */
    public void lockMultiThreadMultiChunk(int operations, int size, int n_threads, int n_chunks, double writeProbability) throws InterruptedException {
        ExecutorService exec = Executors.newFixedThreadPool(n_threads);

        final AtomicLong[] counter = new AtomicLong[n_chunks];
        final long[] cids = new long[n_chunks];
        final long[] ref = new long[n_chunks];
        final long[] value = new long[n_chunks];

        for (int i = 0; i < n_chunks; i++) {
            cids[i] = m.create(size);
            m.writeLong(cids[i], 0, value[0]);
            counter[i] = new AtomicLong(0);
            ref[i] = 0;
        }

        //create the Runnable
        Runnable[] runners = new Runnable[operations];
        for (int i = 0; i < runners.length; i++) {
            runners[i] = () -> {
                //select chunk
                int selectedChunk = (int)(Math.random()*cids.length*100) % cids.length;

                if(Math.random() <= writeProbability){
                    //write access
                    m.writeLock(cids[selectedChunk]);

                    m.writeLong(cids[selectedChunk], 0, ++value[selectedChunk]);

                    //check if there are race conditions
                    if(ref[selectedChunk] + 1 != value[selectedChunk]){
                        System.err.println("write expected: " + (ref[selectedChunk] + 1) + " get: " + value[selectedChunk]);
                        System.exit(3);
                    } else
                        ref[selectedChunk] = value[selectedChunk];

                    m.writeUnlock(cids[selectedChunk]);
                    counter[selectedChunk].getAndIncrement();

                } else{
                    m.readLock(cids[selectedChunk]);
                    long tmp = m.readLong(cids[selectedChunk], 0);
                    //check if we get the newest data
                    if(ref[selectedChunk] != tmp){
                        System.err.println("read expected: " + ref[selectedChunk] + " get: " + tmp);
                        System.exit(2);
                    }
                    m.readUnlock(cids[selectedChunk]);
                }
            };
        }

        //start the threads
        for(Runnable r:runners)
            exec.execute(r);

        //don't start new threads
        exec.shutdown();

        //wait until all threads a terminated
        while (!exec.awaitTermination(24L, TimeUnit.HOURS)) {
            System.out.println("Not yet. Still waiting for termination");
        }

        //check if the counter equals the storage value
        for (int i = 0; i < n_chunks; i++) {
            if (counter[i].get() == m.readLong(cids[i], 0)) {
                System.out.println("expected: " + counter[i].get() + " get: " + m.readLong(cids[i], 0));
            } else {
                System.err.println("expected: " + counter[i].get() + " get: " + m.readLong(cids[i], 0));
                System.exit(1);
            }
        }
        System.out.println("all ok");
    }
}
