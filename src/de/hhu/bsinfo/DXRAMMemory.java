package de.hhu.bsinfo;

import de.hhu.bsinfo.dxram.mem.Testing;


public class DXRAMMemory{

    static Testing t = new Testing();

    public static void main(String[] args) throws InterruptedException {

        String[] data = new String[]{
                " ",
                "tee",
                "testing this now",
                "who let the dog out",
                "big, bigger, the knowledge",
                "florian",
                "linux is beautiful"};



        t.memoryManagementTest(900*1000, 2, 0.9,
                0.9, 0.9,0,1,16,2048);


        //t.createDeleteTest(30*1000, 2, 0.75, 16,48, false);
        //t.createAndWriteStringObjects(10000000, 2, data, true);
        //testLock(100000000, 50,2,0.1, false);

        t.getMemoryManagerComponent().checkForError(false);
        System.out.println(Testing.TestingMeasurements.getAndReset());
        t.destroy();
    }

    private static void testLock(long operations, int size, int threads, double write, final boolean checkHeap) throws InterruptedException {

        //init test heap
        t.initHeap(size,1);

        for (int i = 0; i < 24; i++) {
            t.lockTestSpeed(operations, threads,write);
            if(checkHeap)
                t.getMemoryManagerComponent().checkForError(false);

            t.addChunkAndReset((int) (Math.pow(2,i+1) - Math.pow(2,i)));
        }

        System.out.println(Testing.TestingMeasurements.getAndReset());
    }
}
