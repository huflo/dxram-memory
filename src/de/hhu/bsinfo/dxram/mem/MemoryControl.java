package de.hhu.bsinfo.dxram.mem;

import de.hhu.bsinfo.soh.SmallObjectHeap;
import de.hhu.bsinfo.soh.StorageUnsafeMemory;

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

    public SmallObjectHeap getHeap() {
        return m_memory;
    }

    public CIDTable getCIDTable() {
        return m_table;
    }

    public long[] create(int p_size){
        long address = m_memory.malloc(p_size);
        long lid = m_table.getFreeLID();

        if(address == SmallObjectHeap.INVALID_ADDRESS || lid == -1)
            return null;

        m_table.set(lid, address);

        return new long[]{lid, address};
    }

    public void delete(long p_chunkID){
        long address = m_table.delete(p_chunkID, false);
        if(address == SmallObjectHeap.INVALID_ADDRESS){
            System.out.println(p_chunkID + ": Invalid CID!!!");
            return;
        }

        m_memory.free(address);
    }

    public byte[] get(long p_chunkID){
        long address = m_table.get(p_chunkID);
        int size = m_memory.getSizeBlock(address);
        byte[] data = new byte[size];

        m_memory.readBytes(address, 0, data, 0, size);

        return data;
    }

    public void destroy(){
        m_memory.destroy();
    }


}
