package de.hhu.bsinfo.utils.eval;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Write measured values in a file
 *
 * This class is Thread Safe.
 *
 * @author Florian Hucke (florian.hucke@hhu.de) on 26.02.18
 * @projectname dxram-memory
 */
@SuppressWarnings("WeakerAccess")
public final class MultiThreadWriter {

    private FileChannel fc;
    private AtomicLong position = new AtomicLong(0);

    /**
     * Constructor
     *
     * @param pathToFile Path for the file
     * @param descLine first line in the file with a description of the data
     */
    public MultiThreadWriter(final Path pathToFile, String descLine) {
        try {
            Files.createDirectories(pathToFile.getParent());
            fc = FileChannel.open(pathToFile, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            add(descLine + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Add a measure
     *
     * @param fixSize The constant value (x-axis)
     * @param variableSize The variable value (y-axis)
     */
    public final void add(final long fixSize, final long variableSize) {
        add(String.format("[%d,%d],", fixSize, variableSize).getBytes());
    }

    /**
     * Add a measure
     *
     * @param variableSize The variable value (y-axis)
     */
    public final void add(final long variableSize) {
        add((Long.toString(variableSize) + ","));

    }

    /**
     * Add a String to the file
     * @param str String to add
     */
    public final void add(String str) {
        add(str.getBytes());
    }

    /**
     * Add a byte array to the file
     *
     * @param bytes Bytes to add
     */
    public final void add(final byte[] bytes){
        try {
            fc.write(ByteBuffer.wrap(bytes), position.getAndAdd(bytes.length));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public final void close() throws IOException {
        fc.close();
    }
}
