package de.hhu.bsinfo;

import de.hhu.bsinfo.dxram.mem.MemoryTesting;


public class DXRAMMemory{

    static MemoryTesting t = new MemoryTesting((short) 0, (long)Math.pow(2,30), (int)Math.pow(2,22));

    public static void main(String[] args) throws InterruptedException {

        t.pinningFunctional(10000);
        t.destroy();
        System.exit(99);
    }

}
