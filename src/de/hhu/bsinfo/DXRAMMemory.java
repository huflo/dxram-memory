package de.hhu.bsinfo;

import de.hhu.bsinfo.dxram.mem.CIDTable;
import de.hhu.bsinfo.soh.SmallObjectHeap;
import de.hhu.bsinfo.soh.StorageUnsafeMemory;

public class DXRAMMemory{
    private static CIDTable table;
    private static SmallObjectHeap heap;

    public static void main(String[] args){
        long heapSize = 1024*1024*1024; //1GB
        int blockSize = 4*1024*1024;
        short nodeID = 0;


        heap = new SmallObjectHeap(new StorageUnsafeMemory(), heapSize, blockSize);
        table = new CIDTable(nodeID);
        table.initialize(heap);

        table.printDebugInfos();



        destroy();
    }

    private static void destroy(){
        heap.destroy();
    }
}