package de.hhu.bsinfo.dxram.mem;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.utils.FastByteUtils;
import de.hhu.bsinfo.utils.eval.MultiThreadMeasurementHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Florian Hucke (florian.hucke@hhu.de) on 06.03.18
 * @projectname dxram-memory
 */
@SuppressWarnings("unused")
public class Evaluation {
    private static final Logger LOGGER = LogManager.getFormatterLogger(Evaluation.class.getSimpleName());
    private static final DateFormat df = new SimpleDateFormat("yyyy-MM-dd_-_HH-mm");

    private final MemoryManager memoryManager;
    private final String resultFolder;

    //Variable for multi threading
    //Prevent Java heap exceptions by too many Runnables
    private final LinkedBlockingQueue<Runnable> runnables= new LinkedBlockingQueue<>(100000);
    private final RejectedExecutionHandler rejectedExecutionHandler = (runnable, threadPoolExecutor) -> {
        if (!threadPoolExecutor.isShutdown()) {
            //wait until runnable is added
            do {
                if (threadPoolExecutor.getQueue().offer(runnable))
                    break;

            } while (true);

        }
    };

    /**
     * Constructor
     *
     * @param p_memoryManager The memory unit
     * @param p_resultPath The path for  measurement results
     */
    public Evaluation(final MemoryManager p_memoryManager, final String p_resultPath) {
        memoryManager = p_memoryManager;

        String tmpPath =  p_resultPath;
        if(!tmpPath.endsWith("/"))
            tmpPath += "/";

        tmpPath += p_resultPath + df.format(new Date()) + "/";

        resultFolder = tmpPath;
    }


    /**
     * Extended memory test to emulate real life access
     *
     * @param nOperations
     *          Operation count
     * @param nThreads
     *          Number of Threads
     * @param initialChunks
     *          The initial chunks
     * @param initMinSize
     *          Minimal initial chunk size
     * @param initMaxSize
     *          Maximal initial chunk size
     * @param createProbability
     *          Probability of a create access (complement is a delete access)
     * @param readProbability
     *          Probability of a read access (complement is a write access)
     * @param changeProbability
     *          Probability of a data change (complement is a create/delete access)
     * @param minDelay
     *          Minimal delay between operations
     * @param maxDelay
     *          Maximal delay between operations
     * @param minSize
     *          Minimal byte size for a object
     * @param maxSize
     *          Maximal byte size for a object
     */
    public final void accessSimulation(final long rounds, final long nOperations, final int nThreads, final long initialChunks,
                                       final int initMinSize, final int initMaxSize,
                                       final double createProbability, final double readProbability,
                                       final double changeProbability, final long minDelay, final long maxDelay,
                                       final int minSize, final int maxSize) throws InterruptedException, IOException {

        String desc = String.format("operations: %d, threads: %d, init chunks: %d, inti size: [min: %d ,max: %d], " +
                "probabilities: [create: %f, read: %f, change: %f], delay:[min: %d, max: %d], size:[min: %d, max:%d]",
                nOperations, nThreads, initialChunks, initMinSize, initMaxSize, createProbability, readProbability, 
                changeProbability, minDelay, maxDelay, minSize, maxSize);

        System.out.println(desc);
        
        //FunctionalInterface for incrementing the value (with a strong consistency)
        //ByteDataManipulation increment = (byte[] oldData) -> FastByteUtils.longToBytes(FastByteUtils.bytesToLong(oldData) + 1);

        AtomicLong putCounter = new AtomicLong(0);

        
        //Operation counter
        MultiThreadMeasurementHelper measurementHelper = new MultiThreadMeasurementHelper(resultFolder, desc,
                "run", "read", "write", "create", "remove");
        
        MultiThreadMeasurementHelper.Measurement run = measurementHelper.getMeasurement("run");
        MultiThreadMeasurementHelper.Measurement read = measurementHelper.getMeasurement("read");
        MultiThreadMeasurementHelper.Measurement write = measurementHelper.getMeasurement("write");
        MultiThreadMeasurementHelper.Measurement create = measurementHelper.getMeasurement("create");
        MultiThreadMeasurementHelper.Measurement remove = measurementHelper.getMeasurement("remove");



        final byte[] data = FastByteUtils.longToBytes(0);
        long cid;

        Runnable r = () -> {
            long runStart = System.nanoTime();
            wait(minDelay, maxDelay);

            long randomCID = getRandom(1, memoryManager.memoryInformation.getHighestUsedLocalID());
            if(Math.random() < changeProbability){
                if(Math.random() < readProbability){
                    //read data
                    long readStart = System.nanoTime();
                    byte[] b = memoryManager.get(randomCID);
                    read.addTime(b!=null, readStart);
                } else {
                    long writeStart = System.nanoTime();
                    boolean ok = memoryManager.put(randomCID, FastByteUtils.longToBytes(putCounter.getAndIncrement()));
                    write.addTime(ok, writeStart);
                }
            } else {
                if(Math.random() < createProbability){
                    //create
                    long createStart = System.nanoTime();
                    long c = memoryManager.create((int)getRandom(minSize, maxSize));
                    create.addTime(c != ChunkID.INVALID_ID, createStart);

                } else{
                    long removeStart = System.nanoTime();
                    long s = memoryManager.remove(randomCID, false);
                    remove.addTime(s != -1, removeStart);
                }
            }

            run.addTime(true, runStart);
        };


        for (int i = 0; i < rounds; i++) {
            //cleanup old chunks
            for (int j = 0; j < memoryManager.memoryInformation.numActiveChunks; j++) {
                memoryManager.remove(j, false);
            }

            //Create initial chunks
            for (int j = 0; j < initialChunks; j++) {
                cid = memoryManager.create((int) getRandom(initMinSize, initMaxSize));
                memoryManager.put(cid, data);
            }

            long delta;
            long start = System.nanoTime();
            execNOperationsRunnables(nThreads, nThreads, nOperations, r);
            delta = System.nanoTime()-start;

            System.out.println("Time: " + delta);
            measurementHelper.writeStats();
            measurementHelper.newRound();
        }
    }


    /**
     * Execute a given Runnable n-times
     *
     * @param coreThreads The minimal number of threads
     * @param maxThreads The maximal number of threads
     * @param operations Number of operations to run
     * @param runnable The runnable to run n-times
     * @throws InterruptedException Termination can throw this exception
     */
    private void execNOperationsRunnables(final int coreThreads, final int maxThreads, final long operations,
                                          final Runnable runnable) throws InterruptedException {
        ThreadPoolExecutor exec = new ThreadPoolExecutor(coreThreads, maxThreads,24, TimeUnit.HOURS,
                runnables, rejectedExecutionHandler);

        //start the threads
        for (long i = 0; i < operations; i++) {
            exec.execute(runnable);
        }

        //don't start new threads
        exec.shutdown();

        //wait until all threads a terminated
        while (!exec.awaitTermination(24L, TimeUnit.HOURS)) {
            LOGGER.info("Not yet. Still waiting for termination");
        }

    }

    /**
     * Execute a runnable in a time window again and again
     *
     * @param coreThreads The minimal number of threads
     * @param maxThreads The maximal number of threads
     * @param maxTime The time to run the Runnables
     * @param runnable The runnable to run again and again
     * @throws InterruptedException Termination can throw this exception
     */
    private void execMaxTimeRunnables(final int coreThreads, final int maxThreads, final long maxTime,
                                      final Runnable runnable) throws InterruptedException {
        long stopTime = System.currentTimeMillis() + maxTime;

        ThreadPoolExecutor exec = new ThreadPoolExecutor(coreThreads, maxThreads,24, TimeUnit.HOURS,
                runnables, rejectedExecutionHandler);

        //start the threads
        while (System.currentTimeMillis() < stopTime) {
            exec.execute(runnable);
        }

        //don't start new threads
        exec.shutdown();

        //wait until all threads a terminated
        while (!exec.awaitTermination(24L, TimeUnit.HOURS)) {
            LOGGER.info("Not yet. Still waiting for termination");
        }
    }

    /**
     * Wait a random time
     *
     * @param minValue Minimal time to wait
     * @param maxValue Maximal time to wait
     */
    private void wait(final long minValue, final long maxValue){
        try {
            Thread.sleep(getRandom(minValue, maxValue));
        } catch (InterruptedException ignored) {

        }
    }

    /**
     * Get a random number in the range [minValue, maxValue]
     *
     * @param minValue Minimal number
     * @param maxValue Maximal number
     * @return A random number of [minValue, maxValue]
     */
    private long getRandom(long minValue, long maxValue){
        return minValue + (long)(Math.random() * (maxValue - minValue));
    }

}
