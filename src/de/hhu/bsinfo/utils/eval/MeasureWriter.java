package de.hhu.bsinfo.utils.eval;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Write measured values asynchronously in a file
 *
 * @author Florian Hucke (florian.hucke@hhu.de) on 26.02.18
 * @projectname dxram-memory
 */
public final class MeasureWriter {

    private AsynchronousFileChannel afc;
    private AtomicLong position = new AtomicLong(0);

    /**
     * Constructor
     *
     * @param pathToFile Path string for the file
     * @param initLine Descriptive first line
     */
    public MeasureWriter(final String pathToFile, final String initLine){
        try {
            afc = AsynchronousFileChannel.open(Paths.get(pathToFile), StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        } catch (IOException e) {
            e.printStackTrace();
        }

        byte[] b = initLine.getBytes();
        afc.write(ByteBuffer.wrap(b), position.getAndAdd(b.length));
    }

    /**
     * Add a measure
     *
     * @param fixSize The constant value (x-axis)
     * @param variableSize The variable value (y-axis)
     */
    public final void add(final long fixSize, final long variableSize){
        byte[] b = String.format("[%d,%d],", fixSize, variableSize).getBytes();

        afc.write(ByteBuffer.wrap(b), position.getAndAdd(b.length));
    }
}
