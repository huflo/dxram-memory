package de.hhu.bsinfo;

import de.hhu.bsinfo.dxram.mem.MemoryControl;
import de.hhu.bsinfo.soh.SmallObjectHeapAnalyzer;

public class DXRAMMemory{

    public static void main(String[] args){
        long heapSize = 1024*1024*1024; //1GB
        short nodeID = 0;

        MemoryControl memoryControl = new MemoryControl(nodeID, heapSize);

        memoryControl.create(50);
        memoryControl.create(50);
        memoryControl.delete(1);

        memoryControl.getCIDTable().printDebugInfos();

        SmallObjectHeapAnalyzer analyzer = new SmallObjectHeapAnalyzer(memoryControl.getHeap());
        System.out.println(analyzer.analyze().toString());

        memoryControl.destroy();
    }
}