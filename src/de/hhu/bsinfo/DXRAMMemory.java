package de.hhu.bsinfo;

import de.hhu.bsinfo.dxram.mem.MemoryEvaluation;
import de.hhu.bsinfo.dxram.mem.MemoryManager;

public class DXRAMMemory{

    public static void main(String[] args) {

        if(args.length == 0)
            args = new String[]{"0", "1073741824", "4194304", "true", "true", "master", "3", "100", "2", "1000", "16", "48",
                    "0.05", "0.8", "0.1", "0", "1", "16", "2048"};

        if(args.length < 19){
            System.out.println("Run: nodeID heapSize blockSize readLock writeLock branchName nOperations nThreads initialChunks initMinSize initMaxSize\n" +
                    "createProbability readProbability writeProbability minDelayInMS maxDelay\n" +
                    "minSize maxSizeInByte" );

            System.exit(-1);
        }

        int arg = 0;
        short nodeID = Short.parseShort(args[arg++]);
        long heapSize = Long.parseLong(args[arg++]);
        int blockSize = Integer.parseInt(args[arg++]);

        boolean readLock = Boolean.parseBoolean(args[arg++]);
        boolean writeLock = Boolean.parseBoolean(args[arg++]);

        String branch = args[arg++];
        int rounds = Integer.parseInt(args[arg++]);
        long nOperations = Long.parseLong(args[arg++]);
        int nThreads = Integer.parseInt(args[arg++]);
        int initialChunks = Integer.parseInt(args[arg++]);
        int initMinSize = Integer.parseInt(args[arg++]);
        int initMaxSize = Integer.parseInt(args[arg++]);
        double createProbability = Double.parseDouble(args[arg++]);
        double readProbability = Double.parseDouble(args[arg++]);
        double writeProbability = Double.parseDouble(args[arg++]);
        long minDelayInMS = Long.parseLong(args[arg++]);
        long maxDelay = Long.parseLong(args[arg++]);
        int minSize = Integer.parseInt(args[arg++]);
        int maxSizeInByte = Integer.parseInt(args[arg]);

        MemoryManager memoryManager = new MemoryManager(nodeID, heapSize, blockSize);
        MemoryEvaluation eval = new MemoryEvaluation(memoryManager, "./eval/" + branch + "/", initialChunks, initMinSize, initMaxSize);
        eval.setLocks(readLock, writeLock);
        eval.setRounds(rounds);
        eval.setOperations(nOperations);
        eval.setThreads(nThreads);

        eval.accessSimulation(createProbability, readProbability, writeProbability, minSize, maxSizeInByte);

        memoryManager.shutdownMemory();
    }
}
