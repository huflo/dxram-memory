package de.hhu.bsinfo;

import de.hhu.bsinfo.dxram.mem.Evaluation;
import de.hhu.bsinfo.dxram.mem.MemoryManager;

import java.io.IOException;

public class DXRAMMemory{

    public static void main(String[] args) throws InterruptedException, IOException {

        if(args.length < 16){
            System.out.println("Run: nodeID heapSize blockSize nOperations, nThreads, initialChunks, initMinSize, initMaxSize,\n" +
                    "createProbability, readProbability, changeProbability, minDelayInMS, maxDelay,\n" +
                    "minSize, maxSizeInByte" );

            System.exit(-1);
        }

        int arg = 0;
        short nodeID = Short.parseShort(args[arg++]);
        long heapSize = Long.parseLong(args[arg++]);
        int blockSize = Integer.parseInt(args[arg++]);

        long rounds = Long.parseLong(args[arg++]);
        long nOperations = Long.parseLong(args[arg++]);
        int nThreads = Integer.parseInt(args[arg++]);
        long initialChunks = Long.parseLong(args[arg++]);
        int initMinSize = Integer.parseInt(args[arg++]);
        int initMaxSize = Integer.parseInt(args[arg++]);
        double createProbability = Double.parseDouble(args[arg++]);
        double readProbability = Double.parseDouble(args[arg++]);
        double changeProbability = Double.parseDouble(args[arg++]);
        long minDelayInMS = Long.parseLong(args[arg++]);
        long maxDelay = Long.parseLong(args[arg++]);
        int minSize = Integer.parseInt(args[arg++]);
        int maxSizeInByte = Integer.parseInt(args[arg]);

        MemoryManager memoryManager = new MemoryManager(nodeID, heapSize, blockSize);
        Evaluation eval = new Evaluation(memoryManager, "./eval/");
        eval.accessSimulation(rounds, nOperations, nThreads, initialChunks, initMinSize, initMaxSize, createProbability,
                readProbability,changeProbability, minDelayInMS, maxDelay, minSize, maxSizeInByte);

        memoryManager.shutdownMemory();
    }
}
