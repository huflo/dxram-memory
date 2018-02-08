package de.hhu.bsinfo;

import de.hhu.bsinfo.dxram.mem.MemoryControl;

public class DXRAMMemory{

    public static void main(String[] args){
        long heapSize = 1024*1024*1024; //1GB
        short nodeID = 0;

        MemoryControl memoryControl = new MemoryControl(nodeID, heapSize);

        memoryControl.addressSizeTest();

        memoryControl.destroy();
    }
}