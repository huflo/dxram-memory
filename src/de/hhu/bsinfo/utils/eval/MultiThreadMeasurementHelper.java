package de.hhu.bsinfo.utils.eval;

import de.hhu.bsinfo.dxutils.stats.ValuePercentile;

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
@SuppressWarnings("SameParameterValue")
public class MultiThreadMeasurementHelper {

    private final Measurement[] measurements;
    private final String folder;
    private final String descLine;
    private static boolean collectMiss = false;

    private static long round = 0;

    /**
     * Constructor
     *
     * @param p_folder Folder to save the measurement results
     * @param p_descLine Description of the measurement
     * @param p_measurements Names of the measurements
     */
    public MultiThreadMeasurementHelper(final String p_folder, final String p_descLine,
                                        final String... p_measurements) {
        this(p_folder, p_descLine, false, p_measurements);

    }
    /**
     * Constructor
     *
     * @param p_folder Folder to save the measurement results
     * @param p_descLine Description of the measurement
     * @param p_measurements Names of the measurements
     * @param p_collectMiss Collect data of failed operations
     */
    public MultiThreadMeasurementHelper(final String p_folder, final String p_descLine, final boolean p_collectMiss,
                                        final String... p_measurements) {
        folder = (p_folder.endsWith("/")?p_folder:(p_folder + "/"));
        descLine = p_descLine;
        collectMiss = p_collectMiss;

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
    public final Measurement getMeasurement(final String name) {
        for (Measurement m:measurements)
            if (m.name.equals(name))
                return m;

        return null;
    }

    /**
     * Start a new round.
     */
    public final void newRound() {
        round++;
    }



    /**
     * Write collected stats to a file
     *
     * @throws IOException The FileChannel can throw a IOException
     */
    public final void writeStats() throws IOException {
        MultiThreadWriter mtw = new MultiThreadWriter(Paths.get(folder + "stats_" + round + ".csv"), descLine);
        mtw.add(measurements[1].csvHeader(','));
        mtw.add(getCSVStats(','));
        mtw.close();
    }

    /**
     * Get all stats of the measurements
     *
     * @param delim The separator
     * @return A string of all measurements
     */
    private String getCSVStats(final char delim) {
        StringBuilder out = new StringBuilder();

        for (Measurement m:measurements)
            out.append(m.getExecutedSuccessfullyStats(delim));

        return out.toString();
    }


    /**
     * Single measurement
     *
     * @author Florian Hucke (florian.hucke@hhu.de) on 06.03.18
     */
    public final class Measurement{
        private final String name;

        private final ValuePercentile percentileHit;
        private final ValuePercentile percentileMiss;

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
            percentileHit = new ValuePercentile(MultiThreadMeasurementHelper.Measurement.class, name);
            percentileMiss = new ValuePercentile(MultiThreadMeasurementHelper.Measurement.class, name);
        }

        /**
         * Add a measured Time.
         *
         * @param ok check if a hit or a miss occurred
         * @param deltaTime Time delta
         */
        public final void addTime(final boolean ok, final long deltaTime) {
            if(ok){
                hit.incrementAndGet();
                hit_accu_time.getAndAdd(deltaTime);
                hit_best.accumulateAndGet(deltaTime, Math::min);
                hit_worst.accumulateAndGet(deltaTime, Math::max);

                synchronized (percentileHit){
                    percentileHit.record(deltaTime);
                }
            } else if (collectMiss){
                miss.incrementAndGet();
                miss_accu_time.getAndAdd(deltaTime);
                miss_best.accumulateAndGet(deltaTime, Math::min);
                miss_worst.accumulateAndGet(deltaTime, Math::max);

                synchronized (percentileMiss) {
                    percentileMiss.record(deltaTime);
                }
            }
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
        final String getStats(){
            char delim = ',';
            return getExecutedSuccessfullyStats(delim) +
                    ((collectMiss)?getNotExecutedSuccessfullyStats(delim):"");

        }

        /**
         * get the statistics of successfully completed tests in CSV format
         *
         * @param delim The separator
         * @return The stats in CSV format
         */
        final String getExecutedSuccessfullyStats(final char delim) {
            StringBuilder out = new StringBuilder();
            if(hit.get() > 0) {
                out.append(name).append(delim).append(hit.get()).append(delim)
                        .append(hit_best.get()).append(delim).append(hit_worst.get())
                        .append(delim);

                if (hit.get() > 0)
                    out.append(hit_accu_time.get() / hit.get());
                else
                    out.append(0);

                out.append(delim).append(percentileHit.toCSV(delim)).append('\n');
            }

            return out.toString();
        }

        /**
         * get the statistics of successfully completed tests in CSV format
         *
         * @param delim The separator
         * @return The stats in CSV format
         */
        final String getNotExecutedSuccessfullyStats(final char delim) {
            StringBuilder out = new StringBuilder();
            if(miss.get() > 0) {
                out.append(name).append("(miss)").append(delim).append(miss.get()).append(delim)
                        .append(miss_best.get()).append(delim).append(miss_worst.get())
                        .append(delim);

                if (miss.get() > 0)
                    out.append(miss_accu_time.get() / miss.get());
                else
                    out.append(0);

                out.append(delim).append(percentileMiss.toCSV(delim)).append('\n');
            }

            return out.toString();
        }

        /**
         * Create a CSV header.
         *
         * @param delim The separator
         * @return a header for the CSV file
         */
        private String csvHeader(final char delim){
            return "name" + delim + "operation" + delim + "best" + delim + "worst" + delim + "average"
                    + delim + percentileHit.generateCSVHeader(delim) + '\n';
        }
    }
}
