package de.hhu.bsinfo.dxram.mem;

import de.hhu.bsinfo.utils.BitMask;

/**
 * Central place for the CIDTable entry logic
 *
 * @author Florian Hucke (florian.hucke@hhu.de) on 16.02.18
 * @projectname dxram-memory
 */
public class CIDTableConfig {

    //43 Bit: the address size of a chunk
    static final CIDTableConfig.Entry ADDRESS = CIDTableConfig.Entry.create(44);

    //10 Bit: as external length field
    static final CIDTableConfig.Entry LENGTH_FIELD = CIDTableConfig.Entry.create(10);

    // 7 Bit: Count the parallel read access
    static final CIDTableConfig.Entry READ_ACCESS = CIDTableConfig.Entry.create(7);
    static final long READ_INCREMENT = 1L << READ_ACCESS.OFFSET;

    // 1 Bit: Mark a wanted write access
    static final CIDTableConfig.Entry WRITE_ACCESS = CIDTableConfig.Entry.create(1);

    // 1 Bit: no remove allowed (e.g. to purpose a fast path)
    static final CIDTableConfig.Entry STATE_NOT_REMOVEABLE = CIDTableConfig.Entry.create(1);

    // 1 Bit: no move allowed (e.g. to purpose defragmentation)
    static final CIDTableConfig.Entry STATE_NOT_MOVEABLE = CIDTableConfig.Entry.create(1);

    //not moveable implies not removeable so we can use this combination for a full list or a unused cid
    static final long FULL_FLAG = STATE_NOT_MOVEABLE.BITMASK | STATE_NOT_REMOVEABLE.BITMASK;

    /**
     * Handle bit masks and data offset for level 0 entries
     */
    public static final class Entry {
        private static BitMask bm = new BitMask(Long.SIZE);

        public final long BITMASK;
        public final byte OFFSET;
        public final byte SIZE;

        /**
         * Constructor
         *
         * @param neededBits Needed bit for the entry
         */
        private Entry(final byte neededBits){
            OFFSET = bm.getUsedBits();
            BITMASK = bm.checkedCreate(neededBits);
            SIZE = neededBits;
        }

        /**
         * Create a bit partition for a level 0 entry
         *
         * @param neededBits number of bits the entry need
         * @return a Entry Object
         */
        private static Entry create(final int neededBits){
            return new Entry((byte)neededBits);
        }

        /**
         * Get the saved data from a entry
         *
         * @param p_tableEntry the level 0 table entry
         * @return the saved data
         */
        public final long get(final long p_tableEntry){
            return (p_tableEntry & BITMASK) >> OFFSET;
        }
    }
}
