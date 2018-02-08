package de.hhu.bsinfo.dxram.mem;

import de.hhu.bsinfo.soh.MemoryRuntimeException;
import de.hhu.bsinfo.soh.SmallObjectHeap;
import de.hhu.bsinfo.soh.SmallObjectHeapAnalyzer;
import de.hhu.bsinfo.soh.StorageUnsafeMemory;
import de.hhu.bsinfo.utils.BitMask;

/**
 * @author Florian Hucke (florian.hucke@hhu.de) on 06.02.18
 * @projectname dxram-memory
 *
 * Minimal class to make operations available. This class is a thin replacement of the MemoryManagementComponent
 */
public final class MemoryControl {
    private final CIDTable m_table;
    private final SmallObjectHeap m_memory;
    private final int blockSize = 4*1024*1024; //4MB

    public MemoryControl(short p_nodeID, long p_size){
        m_memory = new SmallObjectHeap(new StorageUnsafeMemory(), p_size, blockSize);
        m_table = new CIDTable(p_nodeID);
        m_table.initialize(m_memory);
    }

    //general getter

    /**
     * Heap getter
     * @return the active SmallObjectHeap instance
     */
    public SmallObjectHeap getHeap() {
        return m_memory;
    }

    /**
     * CIDTable getter
     * @return the active CIDTable instance
     */
    public CIDTable getCIDTable() {
        return m_table;
    }

    //CIDTable operations

    /**
     * CIDTable only test instance
     *
     * Testing the heap the address part of the LID
     */
    public void addressSizeTest(){
        long address = BitMask.createMask(39, 0);

        //always use the first cid cause we have not enough RAM for a heap which can hold the table
        //go in 16 bit steps else it took a huge amount of time
        System.out.println("Notice: We except a overflow at: " + String.format("%016X", BitMask.createMask(43, 0)));
        while(setAndGet(address+=65536, 1) && address <= BitMask.createMask(1,43));
    }

    /**
     * Test for the get and set method of the CIDTable class
     *
     * @param address address we want to
     * @param cid the cid we want to use
     */
    public boolean setAndGet(long address, long cid){
        StringBuilder outStr = new StringBuilder();

        try{
            m_table.set(cid, address);
        } catch (MemoryRuntimeException e){
            e.printStackTrace();
            System.err.println("Address: " + address + " CID: " + cid);

            System.out.println(m_table.getTableCount());

            return false;
        }
        long address2 = m_table.get(cid);
        if(address != address2){
            outStr.append("Get LID:")
                    .append(String.format("0x%016X", cid))
                    .append('\n')
                    .append("Address error-->address: ")
                    .append(String.format("0x%016X", address))
                    .append(" is unequal to ")
                    .append(String.format("0x%016X", address2))
                    .append('\n');
            System.err.println(outStr.toString());
        }

        m_table.delete(cid, false);

        return true;
    }

    /**
     * create a chunk with data
     * @param p_size size of the chunk
     * @param p_data data of the chunk
     * @return the cid and address as long array
     */
    private long[] createWithData(int p_size, byte[] p_data){
        long[] chunk = create(p_size);
        if(chunk == null)
            return  null;

        m_memory.writeBytes(chunk[1], 0, p_data, 0, p_data.length);

        return chunk;
    }

    public void readLIDTable(long address){
        for (int i = 0; i < CIDTable.ENTRIES_PER_LID_LEVEL; i++){
            long entry = m_table.readEntry(address + i * 8, 0);
            System.out.println(String.format("0x%016X", entry));
        }
    }

    /**
     * Create a chunk
     * @param p_size size of the chunk
     * @return the cid and address as long array
     */
    public long[] create(int p_size){
        long address, lid;

        if((address = m_memory.malloc(p_size)) == SmallObjectHeap.INVALID_ADDRESS){
            System.err.println("address is invalid -> break");
            return null;
        }

        if((lid = m_table.getFreeLID()) == -1){
            System.err.println("lid is invalid -> break");
            m_memory.free(address);
            return null;
        }
        m_table.set(lid, address);

        return new long[]{lid, address};
    }

    /**
     * delete a chunk
     * @param p_chunkID cid we want delete
     */
    public void delete(long p_chunkID){
        long address = m_table.delete(p_chunkID, false);
        if(address == SmallObjectHeap.INVALID_ADDRESS){
            System.out.println(p_chunkID + ": Invalid CID!!! (@" + address + ")");
            return;
        }

        m_memory.free(address);
    }

    /**
     * get a chunk by cid
     * @param p_chunkID cid we want get
     * @return data of the chunk
     */
    public byte[] get(long p_chunkID){
        long address = m_table.get(p_chunkID);
        int size = m_memory.getSizeBlock(address);
        byte[] data = new byte[size];

        m_memory.readBytes(address, 0, data, 0, size);

        return data;
    }


    /**
     * debug memory control
     */
    public void debug(){
        m_table.printDebugInfos();
        analyzeHeap();
    }

    /**
     * Run a heap analyze
     */
    public void analyzeHeap(){
        SmallObjectHeapAnalyzer analyzer = new SmallObjectHeapAnalyzer(m_memory);
        System.out.println(analyzer.analyze().toString());
    }

    /**
     * destroy CIDTable and the SmallObject heap instance
     */
    public void destroy(){
        m_table.disengage();
        m_memory.destroy();
    }


}
