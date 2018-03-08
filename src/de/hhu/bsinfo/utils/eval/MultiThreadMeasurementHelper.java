package de.hhu.bsinfo.utils.eval;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Do multiple time measures in a Multi-Thread environment and write all
 * collected data to a specific folder.
 *
 * @author Florian Hucke (florian.hucke@hhu.de) on 06.03.18
 * @projectname dxram-memory
 */
public class MultiThreadMeasurementHelper {

    private final Measurement[] measurements;
    private final String folder;
    private final String descLine;

    private static long round = 0;

    /**
     * Constructor
     *
     * @param p_folder Folder to save the measurement results
     * @param p_descLine Description of the measurement
     * @param p_measurements Names of the measurements
     */
    public MultiThreadMeasurementHelper(String p_folder, String p_descLine, String... p_measurements) {
        folder = (p_folder.endsWith("/")?p_folder:(p_folder + "/"));
        descLine = p_descLine;

        measurements = new Measurement[p_measurements.length];
        for (int i = 0; i < p_measurements.length; i++) {
            measurements[i] = new Measurement(p_measurements[i]);
        }
    }

    /**
     * Get a Measurement instance
     *
     * @param name Name of the measurement
     * @return The measurement instance of null if no suitable instance was found
     */
    public Measurement getMeasurement(String name) {
        for (Measurement m:measurements)
            if (m.name.equals(name))
                return m;

        return null;
    }

    /**
     * Start a new round.
     *
     * @throws IOException The FileChannel can throw a IOException
     */
    public void newRound() throws IOException {
        round++;
        for (Measurement m:measurements)
            m.createFile();
    }

    /**
     * Get all stats of the measurements
     *
     * @return A String of all measurements.
     */
    public String getStats(){
        StringBuilder out = new StringBuilder();
        for (Measurement m:measurements)
            out.append(m);

        return out.toString();
    }

    /**
     * Single measurement
     *
     * @author Florian Hucke (florian.hucke@hhu.de) on 06.03.18
     */
    public class Measurement{
        private String name;
        private MultiThreadWriter m_multiThreadWriter;

        private final AtomicLong hit = new AtomicLong(0);
        private final AtomicLong hit_accu_time = new AtomicLong(0);
        private final AtomicLong hit_best = new AtomicLong(Long.MAX_VALUE);
        private final AtomicLong hit_worst = new AtomicLong(0);

        private final AtomicLong miss = new AtomicLong(0);
        private final AtomicLong miss_accu_time = new AtomicLong(0);
        private final AtomicLong miss_best = new AtomicLong(Long.MAX_VALUE);
        private final AtomicLong miss_worst = new AtomicLong(0);

        /**
         * Constructor
         *
         * @param p_name Name of the measure
         */
        Measurement(final String p_name) {
            name = p_name;
            try {
                createFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * Add a measured Time.
         *
         * @param ok check if a hit or a miss occurred
         * @param startTime the start time of the operation
         */
        public void addTime(final boolean ok, final long startTime) {
            long time = System.nanoTime()-startTime;
            if(ok){
                hit.incrementAndGet();
                hit_accu_time.getAndAdd(time);
                hit_best.accumulateAndGet(time, Math::min);
                hit_worst.accumulateAndGet(time, Math::max);
            } else {
                miss.incrementAndGet();
                miss_accu_time.getAndAdd(time);
                miss_best.accumulateAndGet(time, Math::min);
                miss_worst.accumulateAndGet(time, Math::max);
            }
            m_multiThreadWriter.add(time);
        }

        /**
         * Create a file and close the previous one
         *
         * @throws IOException The FileChannel can throw a IOException
         */
        private void createFile() throws IOException {
            if(m_multiThreadWriter != null)
                m_multiThreadWriter.close();

            m_multiThreadWriter = new MultiThreadWriter(Paths.get(folder + name + "_" + round), descLine);
        }

        @Override
        public String toString() {
            return getStats();
        }

        /**
         * Create a string of all hit and miss operations with the best, the worst and the average time.
         * This information about miss operations are only showed when at least one miss operation occurred
         *
         * @return A String
         */
        String getStats(){
            StringBuilder out = new StringBuilder(name).append('\n');
            if(hit.get() > 0)
                out.append(String.format("hit(%d)\t-> best: %d, worst: %d, avg: %d\n",
                    hit.get(), hit_best.get(), hit_worst.get(), hit_accu_time.get()/hit.get()));

            if(miss.get() > 0)
                out.append(String.format("miss(%d)\t-> best: %d, worst: %d, avg: %d\n",
                        miss.get(), miss_best.get(), miss_worst.get(), miss_accu_time.get()/miss.get()));

            out.append("\n");

            return out.toString();
        }
    }
}
