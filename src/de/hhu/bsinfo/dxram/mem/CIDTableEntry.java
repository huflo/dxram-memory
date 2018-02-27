package de.hhu.bsinfo.dxram.mem;

import de.hhu.bsinfo.utils.BitMask;

/**
 * Central place for the CIDTable entry logic
 *
 * @author Florian Hucke (florian.hucke@hhu.de) on 16.02.18
 * @projectname dxram-memory
 */
public class CIDTableEntry {

    //43 Bit: the address size of a chunk
    static final CIDTableEntry.Entry ADDRESS = CIDTableEntry.Entry.create(43);

    //1 Bit: Object is bigger than 2^10
    static final CIDTableEntry.Entry EMBEDDED_LENGTH_FIELD = CIDTableEntry.Entry.create(1);

    //2 Bit: If object is bigger 2^10 save size of the length field
    static final CIDTableEntry.Entry LENGTH_FIELD_SIZE = CIDTableEntry.Entry.create(2);

    //8 Bit: as external length field if object is bigger 2^10
    static final CIDTableEntry.Entry PARTED_LENGTH_FIELD = CIDTableEntry.Entry.create(8);

    //If object is smaller or equal 2^10 then save no length field.
    static final CIDTableEntry.Entry LENGTH_FIELD = CIDTableEntry.Entry.CreateCombiened(LENGTH_FIELD_SIZE, PARTED_LENGTH_FIELD);

    // 7 Bit: Count the parallel read access
    static final CIDTableEntry.Entry READ_ACCESS = CIDTableEntry.Entry.create(7);
    static final long READ_INCREMENT = 1L << READ_ACCESS.OFFSET;

    // 1 Bit: Mark a wanted write access
    static final CIDTableEntry.Entry WRITE_ACCESS = CIDTableEntry.Entry.create(1);

    // 1 Bit: no remove allowed (e.g. to purpose a fast path)
    static final CIDTableEntry.Entry STATE_NOT_REMOVEABLE = CIDTableEntry.Entry.create(1);

    // 1 Bit: no move allowed (e.g. to purpose defragmentation)
    static final CIDTableEntry.Entry STATE_NOT_MOVEABLE = CIDTableEntry.Entry.create(1);

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

        private Entry(Entry e1, Entry e2){
            OFFSET = (byte) Math.min(e1.OFFSET, e2.OFFSET);
            BITMASK = e1.BITMASK | e2.BITMASK;
            SIZE = (byte) (e1.SIZE + e2.SIZE);

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

        private static Entry CreateCombiened(Entry e1, Entry e2){
            return new Entry(e1,e2);
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
