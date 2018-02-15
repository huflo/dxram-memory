package de.hhu.bsinfo;

import de.hhu.bsinfo.dxram.mem.Testing;

public class DXRAMMemory{

    public static void main(String[] args) throws InterruptedException {
        Testing t;
        for (int i = 0; i < 1; i++) {

            t = new Testing();

            //t.lockFor(100);

            t.lockThreads(100000, 0.1);

            t.destroy();
        }
    }
}