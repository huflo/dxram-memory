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
    private final long cid;
    private final AtomicLong valueAtomic = new AtomicLong(0);
    private final long[] value = {0};

    /**
     * Create a testing instance
     */
    public Testing(){
        m = new MemoryManagerComponent();
        m.initComponent();
        cid = m.create(50);
        m.writeLong(cid, 0, value[0]);
    }

    /**
     * Destroy the testing instance
     */
    public void destroy(){
        m.shutdownComponent();
    }

    /**
     * Test the chunk wise locking
     * 
     * @param n number of threads to use
     * @param writeProbability probability of a write access (0.0 no write access -> 1.0 only write access)
     * @throws InterruptedException internal thread pool can throw a exception while awaiting termination
     */
    public void lockThreads(int n, double writeProbability) throws InterruptedException {
        Runnable[] runnableArray = new Runnable[n];
        ExecutorService exec = Executors.newCachedThreadPool();
        AtomicLong counter = new AtomicLong(0);
        final long[] ref = {0};

        for(int i = 0; i < runnableArray.length; i++)
            runnableArray[i] = () -> {
                if(Math.random() < writeProbability){
                    m.writeLock(cid);

                    m.writeLong(cid, 0, ++value[0]);
                    
                    //check if there are race conditions
                    if(ref[0] + 1 != value[0]){
                        System.err.println("write expected: " + (ref[0] + 1) + " get: " + value[0]);
                        System.exit(3);
                    } else
                        ref[0] = value[0];

                    m.writeUnlock(cid);
                    counter.getAndIncrement();
                } else{
                    m.readLock(cid);
                    long tmp = m.readLong(cid, 0);
                    //check if we get the newest data
                    if(ref[0] != tmp){
                        System.err.println("read expected: " + ref[0] + " get: " + tmp);
                        System.exit(2);
                    }
                    m.readUnlock(cid);
                }
            };

        //start the threads
        for(Runnable r:runnableArray)
            exec.execute(r);

        //don't start new threads
        exec.shutdown();

        //wait until all threads a terminated
        while (!exec.awaitTermination(24L, TimeUnit.HOURS)) {
            System.out.println("Not yet. Still waiting for termination");
        }

        //check if the counter equals the storage value
        if(counter.get() == m.readLong(cid, 0)){
            System.out.println("expected: " + counter.get() + " get: " + m.readLong(cid,0));
            System.out.println("all ok");
        } else{
            System.err.println("expected: " + counter.get() + " get: " + m.readLong(cid,0));
            System.exit(1);
        }

    }

    /**
     * A sequential access simulation
     *
     * @param n number of accesses
     * @param writeProbability probability of a write access (0.0 no write access -> 1.0 only write access)
     */
    public void lockFor(int n, double writeProbability){
        for (int i = 0; i < n; i++) {
            if(Math.random() < writeProbability){
                m.writeLong(cid, 0, ++value[0]);
                m.printTableEntry("W: " + value[0] + " ", cid);
            } else {
                long tmp = m.readLong(cid, 0);
                m.printTableEntry("R: " + tmp + " ", cid);
            }
        }
    }
}
