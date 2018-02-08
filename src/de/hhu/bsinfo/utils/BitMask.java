package de.hhu.bsinfo.utils;

/**
 * A helper class to create bit masks
 *
 * @author Florian Hucke (florian.hucke@hhu.de) on 07.02.18
 * @projectname dxram-memory
 */
public class BitMask {

    final int bitSize;
    long control = 0x0;

    public BitMask(int byteSizeForBitMask){
        bitSize = byteSizeForBitMask * 8;
    }

    /**
     * Create a bit mask. Check if there are a union with previous created bit masks
     * @param numberOfOnes number of ones
     * @param offset offset of the lowest one
     * @return a checked bit mask
     */
    public long checkedCreate(long numberOfOnes, int offset){

        long ret = create(numberOfOnes, offset);

        assert (control & ret) == 0 : "overlapping masks. control = " +
                String.format("0x%016X", control) + " ret = " + String.format("0x%016X", ret);
        control |= ret;

        return ret;
    }

    /**
     * create bit masks
     * @param numberOfOnes number of ones
     * @param offset offset from the LSB
     * @return a bit mask
     */
    public long create(long numberOfOnes, int offset){


        long ret = 0;
        for (int i = 0; i < numberOfOnes; i++) {
            ret = (ret << 1) | 1;
        }

        return ret << offset;
    }

    /**
     * Static method to create a bit mask
     * @param numberOfOnes number of ones
     * @param offset offset of the ones
     * @return a bit mask as long variable
     */
    public static long createMask(long numberOfOnes, int offset){
        assert numberOfOnes+offset <= Long.SIZE : "bit mask result in a overflow";

        long ret = 0;
        for (int i = 0; i < numberOfOnes; i++) {
            ret = (ret << 1) | 1;
        }

        return ret << offset;
    }

}
