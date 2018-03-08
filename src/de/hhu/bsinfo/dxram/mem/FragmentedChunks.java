package de.hhu.bsinfo.dxram.mem;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static de.hhu.bsinfo.dxram.mem.CIDTableEntry.*;

/**
 * Iterate on the CIDTable tables and find fragmented areas
 *
 * @author Florian Hucke (florian.hucke@hhu.de) on 08.03.18
 * @projectname dxram-memory
 */
public class FragmentedChunks implements Runnable{
    private static final Logger LOGGER = LogManager.getFormatterLogger(FragmentedChunks.class.getSimpleName());

    private final CIDTable cidTable;
    private final SmallObjectHeap smallObjectHeap;
    private final long nodeID;

    /**
     * Constructor
     *
     * @param p_memoryManager The central memory unit
     */
    public FragmentedChunks(MemoryManager p_memoryManager){
        cidTable = p_memoryManager.cidTable;
        smallObjectHeap = p_memoryManager.smallObjectHeap;
        nodeID = (long) cidTable.m_ownNodeID << (Long.SIZE-CIDTable.BITS_FOR_NID_LEVEL);
    }

    @Override
    public void run() {
        long lid = 0;
        long entry;
        while (true){
            if(cidTable.getNextLocalIDCounter() < 2)
                continue;

            lid %= (cidTable.getNextLocalIDCounter()-1);
            entry = cidTable.get(lid | nodeID);
            System.out.println(CIDTableEntry.entryData(entry));
            if(entry == CIDTable.ZOMBIE_ENTRY || entry == CIDTable.FREE_ENTRY){ // check entry for validity
                lid++;
                continue;
            }

            byte sur = checkSurrounding(entry);
            System.out.println(sur);
            if(sur == (byte) 0xFF){ // check if we get a error
                LOGGER.info("Error on LID: %d, goto next", lid);
                lid++;
                continue;
            }


            if(sur == 1)
                LOGGER.info("Entry %d has a free block on its left side", entry);
            else if (sur == 2)
                LOGGER.info("Entry %d has a free block on its right side", entry);
            else if (sur == 3)
                LOGGER.info("Entry %d has a free block on both sides", entry);

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                return;
            }

            lid++;
        }

    }

    /**
     * Check the surrounding blocks
     *
     * @param entry Current entry
     * @return 0 if no free block, 1 if left free block,
     * 2 if right free block, 3 if left and right free block
     * and 0xFF if a exception occurred.
     */
    private byte checkSurrounding(long entry){
        byte ret = 0;
        long address = ADDRESS.get(entry);

        try {
            int leftMarker;
            if (EMBEDDED_LENGTH_FIELD.get(entry)) {
                leftMarker = smallObjectHeap.readLeftPartOfMarker(address -
                        LENGTH_FIELD_SIZE.get(entry) - SmallObjectHeap.SIZE_MARKER_BYTE);

            } else {
                leftMarker = smallObjectHeap.readLeftPartOfMarker(ADDRESS.get(entry) - SmallObjectHeap.SIZE_MARKER_BYTE);
            }

            if (leftMarker < SmallObjectHeap.ALLOC_BLOCK_FLAGS_OFFSET || leftMarker == SmallObjectHeap.SINGLE_BYTE_MARKER) {
                ret = 1;
            }

            int rightMarker;
            rightMarker = smallObjectHeap.readRightPartOfMarker(ADDRESS.get(entry) + smallObjectHeap.getSizeDataBlock(entry));

            if (rightMarker < SmallObjectHeap.ALLOC_BLOCK_FLAGS_OFFSET || rightMarker == SmallObjectHeap.SINGLE_BYTE_MARKER) {
                ret |= 2;
            }
        } catch (Exception e){
            return (byte) 0xFF;
        }

        return ret;
    }
}
