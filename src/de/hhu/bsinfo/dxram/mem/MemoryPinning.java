package de.hhu.bsinfo.dxram.mem;

import java.util.Arrays;

import static de.hhu.bsinfo.dxram.mem.CIDTableEntry.*;

/**
 * @author Florian Hucke (florian.hucke@hhu.de) on 03.03.18
 * @projectname dxram-memory
 */
public class MemoryPinning {
    private final SmallObjectHeap smallObjectHeap;
    private final CIDTable cidTable;

    /**
     * Constructor
     *
     * @param memoryManager
     *          The central unit which manages all memory accesses
     *
     */
    MemoryPinning(MemoryManager memoryManager){
        smallObjectHeap = memoryManager.smallObjectHeap;
        cidTable = memoryManager.cidTable;
    }

    /**
     * Pin a chunk for direct access
     *
     * @param chunkID Chunk ID to pin
     * @return The CIDTable entry
     */
    public long pinChunk(final long chunkID){
        long[] entryPosition = cidTable.getAddressOfEntry(chunkID);

        if(cidTable.setState(entryPosition, STATE_NOT_MOVEABLE, true)){
            return WRITE_ACCESS.set(READ_ACCESS.set(cidTable.get(entryPosition), 0), false);
        }

        return 0;
    }

    /**
     * Unpin a Chunks (slow operation need a DFS over all CIDTable tables)
     *
     * @param cidTableEntry CIDTable entry.
     * @return The corresponding chunk ID or -1 if no suitable chunk ID was found
     */
    public long unpinChunk(final long cidTableEntry){
        long cid = cidTable.reverseSearch(cidTableEntry);
        if(cid != 0)
            cidTable.setState(cid, STATE_NOT_MOVEABLE, false);

        return cid;
    }

    /**
     * Get the data for a entry
     *
     * @param cidTableEntry CIDTable entry
     * @return Chunk data
     */
    public byte[] get(final long cidTableEntry){
        long address = ADDRESS.get(cidTableEntry);
        assert smallObjectHeap.assertMemoryBounds(address);

        long cidLF = LENGTH_FIELD.get(cidTableEntry) + 1;
        int size = smallObjectHeap.getSizeBlock(address, cidLF);

        byte[] data = new byte[size];
        smallObjectHeap.readBytes(address, 0, data, 0, size, cidLF);

        return data;
    }

    /**
     * Put data to a entry
     *
     * @param cidTableEntry Entry
     * @param data Data to put
     */
    public void put(final long cidTableEntry, final byte[] data){
        assert data != null;

        long address = ADDRESS.get(cidTableEntry);
        assert smallObjectHeap.assertMemoryBounds(address);

        long cidLF = LENGTH_FIELD.get(cidTableEntry);

        smallObjectHeap.writeBytes(address, 0, data, 0, data.length, cidLF);
    }
}
