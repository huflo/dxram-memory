package de.hhu.bsinfo.dxram.mem;

import de.hhu.bsinfo.dxram.mem.exceptions.MemoryRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static de.hhu.bsinfo.dxram.mem.CIDTableEntry.*;

/**
 * Management of memory accesses to existing objects with known data type
 *
 * @author Florian Hucke (florian.hucke@hhu.de) on 28.02.18
 * @projectname dxram-memory
 */
public class MemoryDirectAccess {
    private final SmallObjectHeap m_rawMemory;
    private final CIDTable m_cidTable;


    /**
     * Constructor
     *
     * @param memoryManager
     *          The central unit which manages all memory accesses
     *
     */
    public MemoryDirectAccess(MemoryManager memoryManager) {
        this.m_rawMemory = memoryManager.smallObjectHeap;
        m_cidTable = memoryManager.cidTable;
    }

    /**
     * Read a single byte from a chunk. Use this if you need to access a very specific value
     * once to avoid reading a huge chunk. Prefer the get-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to read.
     * @param p_offset
     *         Offset within the chunk to read.
     * @return The value read at the offset of the chunk.
     */
    public byte readByte(final long p_chunkID, final int p_offset) throws MemoryRuntimeException {
        try {
            long entry = m_cidTable.get(p_chunkID);
            long address = ADDRESS.get(entry);
            long size = LENGTH_FIELD.get(entry) + 1; //+1 because of the offset
            boolean deleted = FULL_FLAG.get(entry);
            if (address > SmallObjectHeap.INVALID_ADDRESS && ! deleted) {
                return m_rawMemory.readByte(address, p_offset, size);
            } else {
                return -1;
            }
        } catch (final MemoryRuntimeException e) {
            //handleMemDumpOnError(e, true);
            throw e;
        }
    }

    /**
     * Read a single short from a chunk. Use this if you need to access a very specific value
     * once to avoid reading a huge chunk. Prefer the get-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to read.
     * @param p_offset
     *         Offset within the chunk to read.
     * @return The value read at the offset of the chunk.
     */
    public short readShort(final long p_chunkID, final int p_offset) throws MemoryRuntimeException {
        try {
            long entry = m_cidTable.get(p_chunkID);
            long address = ADDRESS.get(entry);
            long size = LENGTH_FIELD.get(entry) + 1; //+1 because of the offset
            boolean deleted = FULL_FLAG.get(entry);
            if (address > SmallObjectHeap.INVALID_ADDRESS && ! deleted) {
                return m_rawMemory.readShort(address, p_offset, size);
            } else {
                return -1;
            }
        } catch (final MemoryRuntimeException e) {
            //handleMemDumpOnError(e, true);
            throw e;
        }
    }

    /**
     * Read a single int from a chunk. Use this if you need to access a very specific value
     * once to avoid reading a huge chunk. Prefer the get-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to read.
     * @param p_offset
     *         Offset within the chunk to read.
     * @return The value read at the offset of the chunk.
     */
    public int readInt(final long p_chunkID, final int p_offset) throws MemoryRuntimeException {
        try {
            long entry = m_cidTable.get(p_chunkID);
            long address = ADDRESS.get(entry);
            long size = LENGTH_FIELD.get(entry) + 1; //+1 because of the offset
            boolean deleted = FULL_FLAG.get(entry);
            if (address > SmallObjectHeap.INVALID_ADDRESS && ! deleted) {
                return m_rawMemory.readInt(address, p_offset, size);
            } else {
                return -1;
            }
        } catch (final MemoryRuntimeException e) {
            //handleMemDumpOnError(e, true);
            throw e;
        }
    }

    /**
     * Read a single long from a chunk. Use this if you need to access a very specific value
     * once to avoid reading a huge chunk. Prefer the get-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to read.
     * @param p_offset
     *         Offset within the chunk to read.
     * @return The value read at the offset of the chunk.
     */
    public long readLong(final long p_chunkID, final int p_offset) throws MemoryRuntimeException {

        try {
            long entry = m_cidTable.get(p_chunkID);
            long address = ADDRESS.get(entry);
            long lengthField = LENGTH_FIELD.get(entry);
            boolean deleted = FULL_FLAG.get(entry);
            if (address > SmallObjectHeap.INVALID_ADDRESS && ! deleted) {
                return m_rawMemory.readLong(address, p_offset, lengthField);
            } else {
                return -1;
            }
        } catch (final MemoryRuntimeException e) {
            //handleMemDumpOnError(e, true);
            throw e;
        }
    }

    /**
     * Write a single byte to a chunk. Use this if you need to access a very specific value
     * once to avoid writing a huge chunk. Prefer the put-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to write.
     * @param p_offset
     *         Offset within the chunk to write.
     * @param p_value
     *         Value to write.
     * @return True if writing chunk was successful, false otherwise.
     */
    public boolean writeByte(final long p_chunkID, final int p_offset, final byte p_value) throws MemoryRuntimeException {
        try {
            long entry = m_cidTable.get(p_chunkID);
            long address = ADDRESS.get(entry);
            long size = LENGTH_FIELD.get(entry) + 1; //+1 because of the offset
            boolean deleted = FULL_FLAG.get(entry);
            if (address > SmallObjectHeap.INVALID_ADDRESS && ! deleted) {
                m_rawMemory.writeByte(address, p_offset, p_value, size);
            } else {
                return false;
            }
        } catch (final MemoryRuntimeException e) {
            //handleMemDumpOnError(e, true);
            throw e;
        }

        return true;
    }

    /**
     * Write a single short to a chunk. Use this if you need to access a very specific value
     * once to avoid writing a huge chunk. Prefer the put-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to write.
     * @param p_offset
     *         Offset within the chunk to write.
     * @param p_value
     *         Value to write.
     * @return True if writing chunk was successful, false otherwise.
     */
    public boolean writeShort(final long p_chunkID, final int p_offset, final short p_value) throws MemoryRuntimeException {
        try {
            long entry = m_cidTable.get(p_chunkID);
            long address = ADDRESS.get(entry);
            long size = LENGTH_FIELD.get(entry) + 1; //+1 because of the offset
            boolean deleted = FULL_FLAG.get(entry);
            if (address > SmallObjectHeap.INVALID_ADDRESS && ! deleted) {
                m_rawMemory.writeShort(address, p_offset, p_value, size);
            } else {
                return false;
            }
        } catch (final MemoryRuntimeException e) {
            //handleMemDumpOnError(e, true);
            throw e;
        }

        return true;
    }

    /**
     * Write a single int to a chunk. Use this if you need to access a very specific value
     * once to avoid writing a huge chunk. Prefer the put-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to write.
     * @param p_offset
     *         Offset within the chunk to write.
     * @param p_value
     *         Value to write.
     * @return True if writing chunk was successful, false otherwise.
     */
    public boolean writeInt(final long p_chunkID, final int p_offset, final int p_value) throws MemoryRuntimeException {
        try {
            long entry = m_cidTable.get(p_chunkID);
            long address = ADDRESS.get(entry);
            long size = LENGTH_FIELD.get(entry) + 1; //+1 because of the offset
            boolean deleted = FULL_FLAG.get(entry);
            if (address > SmallObjectHeap.INVALID_ADDRESS && ! deleted) {
                m_rawMemory.writeInt(address, p_offset, p_value, size);
            } else {
                return false;
            }
        } catch (final MemoryRuntimeException e) {
            //handleMemDumpOnError(e, true);
            throw e;
        }

        return true;
    }

    /**
     * Write a single long to a chunk. Use this if you need to access a very specific value
     * once to avoid writing a huge chunk. Prefer the put-method if more data of the chunk is needed.
     *
     * @param p_chunkID
     *         Chunk id of the chunk to write.
     * @param p_offset
     *         Offset within the chunk to write.
     * @param p_value
     *         Value to write.
     * @return True if writing chunk was successful, false otherwise.
     */
    public boolean writeLong(final long p_chunkID, final int p_offset, final long p_value) throws MemoryRuntimeException {
        try {
            long entry = m_cidTable.get(p_chunkID);
            long address = ADDRESS.get(entry);
            long lengthField = LENGTH_FIELD.get(entry);
            boolean deleted = FULL_FLAG.get(entry);
            if (address > SmallObjectHeap.INVALID_ADDRESS && ! deleted) {
                m_rawMemory.writeLong(address, p_offset, p_value, lengthField);
            } else {
                return false;
            }
        } catch (final MemoryRuntimeException e) {
            //handleMemDumpOnError(e, true);
            throw e;
        }

        return true;
    }
}
