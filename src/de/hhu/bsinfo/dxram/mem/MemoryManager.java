package de.hhu.bsinfo.dxram.mem;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.ChunkIDRanges;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.mem.exceptions.MemoryRuntimeException;
import de.hhu.bsinfo.dxram.mem.exceptions.OutOfKeyValueStoreMemoryException;
import de.hhu.bsinfo.dxram.mem.manipulation.ByteDataManipulation;
import de.hhu.bsinfo.dxram.mem.storage.StorageUnsafeMemory;
import de.hhu.bsinfo.dxutils.serialization.Exportable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importable;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.unit.StorageUnit;

import static de.hhu.bsinfo.dxram.mem.CIDTableEntry.ADDRESS;

/**
 * Managing memory accesses
 *
 * All operations are Thread-Safe
 *
 * @author Florian Hucke (florian.hucke@hhu.de) on 28.02.18
 * @projectname dxram-memory
 */
public class MemoryManager {
    private SmallObjectHeapDataStructureImExporter[] m_imexporter = new SmallObjectHeapDataStructureImExporter[65536];

    final SmallObjectHeap smallObjectHeap;
    final CIDTable cidTable;
    final MemoryAccess access;
    final MemoryDirectAccess directAccess;
    final MemoryInformation info;
    final MemoryManagement management;
    final MemoryPinning pinning;

    private boolean m_readLock = true;
    private boolean m_writeLock = true;
    private boolean m_doReadLock = true;

    public MemoryManager(final short p_nodeID, final long p_heapSize, final int p_maxBlockSize) {
        smallObjectHeap = new SmallObjectHeap(new StorageUnsafeMemory(), p_heapSize, p_maxBlockSize);
        cidTable = new CIDTable(p_nodeID);
        cidTable.initialize(smallObjectHeap);
        access = new MemoryAccess(this);
        directAccess = new MemoryDirectAccess(this);
        info = new MemoryInformation(this);
        management = new MemoryManagement(this);
        pinning = new MemoryPinning(this);

        info.numActiveChunks = 0;
        info.totalActiveChunkMemory = 0;
    }

    //Manage------------------------------------------------------------------------------------------------------------

    /**
     * Shut down the memory manager
     */
    public void shutdownMemory() {
        cidTable.disengage();
        smallObjectHeap.destroy();
    }

    //Memory Access-----------------------------------------------------------------------------------------------------

    /**
     * Get the payload of a chunk/data structure.
     *
     * This operation is Thread-Safe
     *
     * @param p_dataStructure
     *         Data structure to read specified by its ID.
     * @return True if getting the chunk payload was successful, false if no chunk with the ID specified exists.
     */
    public boolean get(final DataStructure p_dataStructure) {
        return access.get(p_dataStructure);
    }

    /**
     * Get the binary data of a chunk when the chunk size is unknown.
     *
     * This operation is Thread-Safe
     *
     * @param p_chunkID
     *         Read the chunk data of the specified ID
     * @return A byte array with payload if getting the chunk payload was successful, null if no chunk with the ID exists.
     */
    public byte[] get(final long p_chunkID) {
        return access.get(p_chunkID);
    }

    /**
     * Get the binary data of a chunk when the chunk size is unknown.
     *
     * This operation is Thread-Safe
     *
     * @param p_chunkID
     *         Read the chunk data of the specified ID
     * @return the number of read bytes
     */
    public int get(final long p_chunkID, final byte[] p_buffer, final int p_offset, final int p_bufferSize) {
        return access.get(p_chunkID, p_buffer, p_offset, p_bufferSize);
    }

    /**
     * Put data of the a data structure/chunk to the memory
     *
     * This operation is Thread-Safe
     *
     * @param p_dataStructure
     *         Data structure to put
     * @return True if putting the data was successful, false if no chunk with the specified id exists
     */
    public boolean put(final DataStructure p_dataStructure) {
        return access.put(p_dataStructure);
    }

    /**
     * Put some data into a chunk.
     *
     * This operation is Thread-Safe
     *
     * @param p_chunkID
     *         Chunk ID for the data to put
     * @param p_data
     *         Chunk data to put
     * @return True if putting the data was successful, false if no chunk with the specified id exists
     */
    public boolean put(final long p_chunkID, final byte[] p_data) {
        return access.put(p_chunkID, p_data);
    }

    /**
     * Put some data into a chunk.
     *
     * This operation is Thread-Safe
     *
     * @param p_chunkID
     *         Chunk ID for the data to put
     * @param p_data
     *         Chunk data to put
     * @param p_offset
     *         Offset for p_data array
     * @param p_length
     *         Number of bytes to put
     * @return True if putting the data was successful, false if no chunk with the specified id exists
     */
    public boolean put(final long p_chunkID, final byte[] p_data, final int p_offset, final int p_length) {
        return access.put(p_chunkID, p_data, p_offset, p_length);
    }

    /**
     * Modify some data in a chunk.
     *
     * This operation is Thread-Safe
     *
     * @param p_chunkID
     *         Chunk ID for the data to put
     * @param byteDataManipulation
     *         A Interface to manipulate the data of the chunk
     * @return True if putting the data was successful, false if no chunk with the specified id exists
     */
    boolean modify(final long p_chunkID, ByteDataManipulation byteDataManipulation) {
        return access.modify(p_chunkID, byteDataManipulation);
    }

    //MemoryPinning
    /**
     * Pin a chunk for direct access
     *
     * @param chunkID Chunk ID to pin
     * @return The CIDTable entry
     */
    public long pinChunk(final long chunkID){
        return pinning.pinChunk(chunkID);
    }

    /**
     * Unpin a Chunks (slow operation need a DFS over all CIDTable tables)
     *
     * @param cidTableEntry CIDTable entry.
     * @return The corresponding chunk ID or -1 if no suitable chunk ID was found
     */
    public long unpinChunk(final long cidTableEntry){
        return pinning.unpinChunk(cidTableEntry);
    }

    /**
     * Get the data for a entry
     *
     * @param cidTableEntry CIDTable entry
     * @return Chunk data
     */
    public byte[] getPinned(final long cidTableEntry){
        return pinning.get(cidTableEntry);
    }

    /**
     * Put data to a entry
     *
     * @param cidTableEntry Entry
     * @param data Data to put
     */
    public void putPinned(final long cidTableEntry, final byte[] data){
        pinning.put(cidTableEntry, data);
    }


    //MemoryManagement

    /**
     * The chunk ID 0 is reserved for a fixed index structure.
     * If the index structure is already created this will delete the old
     * one and allocate a new block of memory with the same id (0).
     *
     * This method is Thread-Safe
     *
     * @param p_size
     *         Size for the index chunk.
     * @return The chunk id 0
     */
    public long createIndex(final int p_size) throws OutOfKeyValueStoreMemoryException, MemoryRuntimeException {
        return management.createIndex(p_size);
    }

    /**
     * Create a new chunk.
     *
     * This method is Thread-Safe
     *
     * @param p_size
     *         Size in bytes of the payload the chunk contains.
     * @return Chunk ID for the allocated chunk
     */
    public long create(final int p_size) throws OutOfKeyValueStoreMemoryException, MemoryRuntimeException {
        return management.create(p_size);
    }

    /**
     * Create a chunk with a specific chunk id (used for migration/recovery).
     *
     * This method is Thread-Safe
     *
     * @param p_chunkId
     *         Chunk id to assign to the chunk.
     * @param p_size
     *         Size of the chunk.
     * @return The chunk id if successful, -1 if another chunk with the same id already exists.
     */
    public long create(final long p_chunkId, final int p_size) throws OutOfKeyValueStoreMemoryException, MemoryRuntimeException {
        return management.create(p_chunkId, p_size);
    }

    /**
     * Batch/Multi create with a list of sizes
     *
     * This method is Thread-Safe
     *
     * @param p_sizes
     *         List of sizes to create chunks for
     * @return List of chunk ids matching the order of the size list
     */
    public long[] createMultiSizes(final int... p_sizes) {
        return management.createMultiSizes(p_sizes);
    }

    /**
     * Batch/Multi create with a list of sizes
     *
     * This method is Thread-Safe
     *
     * @param p_consecutive
     *         True to enforce consecutive chunk ids
     * @param p_sizes
     *         List of sizes to create chunks for
     * @return List of chunk ids matching the order of the size list
     */
    public long[] createMultiSizes(final boolean p_consecutive, final int... p_sizes) {
        return management.createMultiSizes(p_consecutive, p_sizes);
    }

    /**
     * Batch/Multi create with a list of data structures
     *
     * This method is Thread-Safe
     *
     * @param p_dataStructures
     *         List of data structures. Chunk ids are automatically assigned after creation
     */
    public void createMulti(final DataStructure... p_dataStructures) {
        management.createMulti(p_dataStructures);
    }

    /**
     * Batch/Multi create with a list of data structures
     *
     * This method is Thread-Safe
     *
     * @param p_consecutive
     *         True to enforce consecutive chunk ids
     * @param p_dataStructures
     *         List of data structures. Chunk ids are automatically assigned after creation
     */
    public void createMulti(final boolean p_consecutive, final DataStructure... p_dataStructures) {
        management.createMulti(p_consecutive, p_dataStructures);
    }

    /**
     * Batch create chunks
     *
     * @param p_size
     *         Size of the chunks
     * @param p_count
     *         Number of chunks with the specified size
     * @return Chunk id list of the created chunks
     */
    public long[] createMulti(final int p_size, final int p_count) {
        return management.createMulti(p_size, p_count);
    }

    /**
     * Batch create chunks
     *
     * @param p_size
     *         Size of the chunks
     * @param p_count
     *         Number of chunks with the specified size
     * @param p_consecutive
     *         True to enforce consecutive chunk ids
     * @return Chunk id list of the created chunks
     */
    public long[] createMulti(final int p_size, final int p_count, final boolean p_consecutive) {
        return management.createMulti(p_size, p_count, p_consecutive);
    }

    /**
     * Special create and put call optimized for recovery
     * This is a management call and has to be locked using lockManage().
     *
     * @param p_chunkIDs
     *         List of recovered chunk ids
     * @param p_data
     *         Recovered data
     * @param p_offsets
     *         Offset list for chunks to address the data array
     * @param p_lengths
     *         List of chunk sizes
     * @param p_usedEntries
     *         Specifies the actual number of slots used in the array (may be less than p_lengths)
     */
    public void createAndPutRecovered(final long[] p_chunkIDs, final byte[] p_data, final int[] p_offsets, final int[] p_lengths, final int p_usedEntries) {
        management.createAndPutRecovered(p_chunkIDs, p_data, p_offsets, p_lengths, p_usedEntries);
    }

    /**
     * Special create and put call optimized for recovery
     * This is a management call and has to be locked using lockManage().
     *
     * @param p_dataStructures
     *         All data structure to create and put
     * @return number of written bytes
     */
    //TODO LOCKS
    public int createAndPutRecovered(final DataStructure... p_dataStructures) {
        return management.createAndPutRecovered(p_dataStructures);
    }

    public int remove(final long p_chunkID, final boolean p_wasMigrated) {
        return management.remove(p_chunkID, p_wasMigrated);
    }

    /**
     * Pooling the im/exporters to lower memory footprint.
     *
     * @param p_address
     *         Start address of the chunk
     * @return Im/Exporter for the chunk
     */
    SmallObjectHeapDataStructureImExporter getImExporter(final long p_address, final long p_extSize) {
        long tid = Thread.currentThread().getId();
        if (tid > 65536) {
            throw new RuntimeException("Exceeded max. thread id");
        }

        // pool the im/exporters
        SmallObjectHeapDataStructureImExporter importer = m_imexporter[(int) tid];
        if (importer == null) {
            m_imexporter[(int) tid] = new SmallObjectHeapDataStructureImExporter(smallObjectHeap, p_address, 0, p_extSize);
            importer = m_imexporter[(int) tid];
        } else {
            importer.setAllocatedMemoryStartAddress(p_address);
            importer.setExternalSize(p_extSize);
            importer.setOffset(0);
        }

        return importer;
    }

    //Locks-------------------------------------------------------------------------------------------------------------

    /**
     * Read lock, this lock is switchable, with the method
     * setLocks.
     *
     *
     * @param  p_directEntryAddress
     *              Position of the CIDTable entry
     * @return
     *          True if a lock is received or on weak consistency else false
     *
     */
    boolean switchableReadLock(final long p_directEntryAddress){
        if (m_doReadLock) {
            if (m_readLock)
                return cidTable.directReadLock(p_directEntryAddress);
            else
                return cidTable.directWriteLock(p_directEntryAddress);
        }

        return true;
    }

    /**
     * Read unlock, this unlock is switchable, with the method
     * setLocks.
     *
     *
     * @param  p_directEntryAddress
     *              Position of the CIDTable entry
     * @return
     *          True if a unlock was successful or on weak consistency, else false
     *
     */
    boolean switchableReadUnlock(final long p_directEntryAddress){
        if (m_doReadLock) {
            if (m_readLock)
                return cidTable.directReadUnlock(p_directEntryAddress);
            else
                return cidTable.directWriteUnlock(p_directEntryAddress);
        }

        return true;
    }

    /**
     * Write lock, this lock is switchable, with the method
     * setLocks.
     *
     *
     * @param  p_directEntryAddress
     *              Position of the CIDTable entry
     * @return
     *          True if a lock is received, else false
     *
     */
    boolean switchableWriteLock(final long p_directEntryAddress){
        if(m_writeLock)
            return cidTable.directWriteLock( p_directEntryAddress);
        else
            return cidTable.directReadLock( p_directEntryAddress);
    }

    /**
     * Write unlock, this unlock is switchable, with the method
     * setLocks.
     *
     *
     * @param  p_directEntryAddress
     *              Position of the CIDTable entry
     * @return
     *          True if a unlock was successful, else false
     *
     */
    boolean switchableWriteUnlock(final long p_directEntryAddress){
        if(m_writeLock)
            return cidTable.directWriteUnlock( p_directEntryAddress);
        else
            return cidTable.directReadUnlock( p_directEntryAddress);
    }


    /**
     * Determine the type of access lock.
     *
     * @param p_readLock
     *          If true, use a read lock, otherwise use a write lock for read operations
     * @param p_writeLock
     *          If true, use a write lock, otherwise use a read lock  for write operations
     */
    final void setLocks(final boolean p_readLock, final boolean p_writeLock) {
        m_readLock = p_readLock;
        m_writeLock = p_writeLock;
    }

    final void disableReadLock(final boolean disableReadLock) {
        m_doReadLock = !disableReadLock;
    }

    /**
     * @author Florian Hucke (florian.hucke@hhu.de) on 28.02.18
     * @projectname dxram-memory
     */
    public static class MemoryInformation {
        long numActiveChunks;
        long totalActiveChunkMemory;

        private final SmallObjectHeap m_rawMemory;
        private final CIDTable m_cidTable;


        /**
         * Constructor
         *
         * @param memoryManager
         *          The central unit which manages all memory accesses
         *
         */
        MemoryInformation(MemoryManager memoryManager) {
            this.m_rawMemory = memoryManager.smallObjectHeap;
            m_cidTable = memoryManager.cidTable;
        }

        /**
         * Returns the highest LocalID currently in use
         *
         * @return the LocalID
         */
        public long getHighestUsedLocalID() {
            return m_cidTable.getNextLocalIDCounter() - 1;
        }

        /**
         * Returns the ChunkID ranges of all migrated Chunks
         *
         * @return the ChunkID ranges of all migrated Chunks
         */
        public ChunkIDRanges getCIDRangesOfAllMigratedChunks() {
            return m_cidTable.getCIDRangesOfAllMigratedChunks();
        }

        /**
         * Returns the ChunkID ranges of all locally stored Chunks
         *
         * @return the ChunkID ranges
         */
        public ChunkIDRanges getCIDRangesOfAllLocalChunks() {
            return m_cidTable.getCIDRangesOfAllLocalChunks();
        }

        /**
         * Returns whether this Chunk is stored locally or not.
         * This is an access call and has to be locked using lockAccess().
         *
         * @param p_chunkID
         *         the ChunkID
         * @return whether this Chunk is stored locally or not
         */
        public boolean exists(final long p_chunkID) {
            long address;

            try {
                // Get the address from the CIDTable
                address = ADDRESS.get(m_cidTable.get(p_chunkID));
            } catch (final MemoryRuntimeException e) {
                //handleMemDumpOnError(e, true);
                throw e;
            }

            // If address <= 0, the Chunk does not exists in memory
            return address > SmallObjectHeap.INVALID_ADDRESS &&
                    address < m_rawMemory.m_baseFreeBlockList-SmallObjectHeap.SIZE_MARKER_BYTE;
        }

        /**
         * Returns whether this Chunk was migrated here or not
         *
         * @param p_chunkID
         *         the ChunkID
         * @return whether this Chunk was migrated here or not
         */
        public boolean dataWasMigrated(final long p_chunkID) {
            //->return ChunkID.getCreatorID(p_chunkID) != m_boot.getNodeID();
            return ChunkID.getCreatorID(p_chunkID) != m_cidTable.m_ownNodeID;//<<
        }

        /**
         * Removes the ChunkID of a deleted Chunk that was migrated
         * This is a management call and has to be locked using lockManage().
         *
         * @param p_chunkID
         *         the ChunkID
         */
        public void prepareChunkIDForReuse(final long p_chunkID) {
            // more space for another zombie for reuse in LID store?
            if (m_cidTable.putChunkIDForReuse(ChunkID.getLocalID(p_chunkID))) {
                // kill zombie entry
                m_cidTable.delete(p_chunkID, false);
            } else {
                // no space for zombie in LID store, keep him "alive" in table
                m_cidTable.delete(p_chunkID, true);
            }
        }


        /**
         * Get some status information about the memory manager (free, total amount of memory).
         *
         * @return Status information.
         */
        public Status getStatus() {
            Status status = new Status();

            status.m_freeMemory = new StorageUnit(m_rawMemory.getStatus().getFree(), StorageUnit.BYTE);
            status.m_maxChunkSize = new StorageUnit(m_rawMemory.getStatus().getMaxBlockSize(), StorageUnit.BYTE);
            status.m_totalMemory = new StorageUnit(m_rawMemory.getStatus().getSize(), StorageUnit.BYTE);
            status.m_totalPayloadMemory = new StorageUnit(m_rawMemory.getStatus().getAllocatedPayload(), StorageUnit.BYTE);
            status.m_numberOfActiveMemoryBlocks = m_rawMemory.getStatus().getAllocatedBlocks();
            status.m_totalChunkPayloadMemory = new StorageUnit(totalActiveChunkMemory, StorageUnit.BYTE);
            status.m_numberOfActiveChunks = numActiveChunks;
            status.m_cidTableCount = m_cidTable.getTableCount();
            status.m_totalMemoryCIDTables = new StorageUnit(m_cidTable.getTotalMemoryTables(), StorageUnit.BYTE);
            status.m_cachedFreeLIDs = m_cidTable.getNumCachedFreeLIDs();
            status.m_availableFreeLIDs = m_cidTable.getNumAvailableFreeLIDs();
            status.m_newLIDCounter = m_cidTable.getNextLocalIDCounter();

            return status;
        }


        /**
         * Status object for the memory component containing various information
         * about it.
         *
         * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.03.2016
         */
        public static class Status implements Importable, Exportable {
            private StorageUnit m_freeMemory;
            private StorageUnit m_maxChunkSize;
            private StorageUnit m_totalMemory;
            private StorageUnit m_totalPayloadMemory;
            private long m_numberOfActiveMemoryBlocks = -1;
            private long m_numberOfActiveChunks = -1;
            private StorageUnit m_totalChunkPayloadMemory;
            private long m_cidTableCount = -1;
            private StorageUnit m_totalMemoryCIDTables;
            private int m_cachedFreeLIDs = -1;
            private long m_availableFreeLIDs = -1;
            private long m_newLIDCounter = -1;

            /**
             * Default constructor
             */
            public Status() {

            }

            /**
             * Get the amount of free memory
             *
             * @return Free memory
             */
            public StorageUnit getFreeMemory() {
                return m_freeMemory;
            }

            /**
             * Get the max allowed chunk size
             *
             * @return Max chunk size
             */
            public StorageUnit getMaxChunkSize() {
                return m_maxChunkSize;
            }

            /**
             * Get the total amount of memory available
             *
             * @return Total amount of memory
             */
            public StorageUnit getTotalMemory() {
                return m_totalMemory;
            }

            /**
             * Get the total number of active/allocated memory blocks
             *
             * @return Number of allocated memory blocks
             */
            public long getNumberOfActiveMemoryBlocks() {
                return m_numberOfActiveMemoryBlocks;
            }

            /**
             * Get the total number of currently active chunks
             *
             * @return Number of active/allocated chunks
             */
            public long getNumberOfActiveChunks() {
                return m_numberOfActiveChunks;
            }

            /**
             * Get the amount of memory used by chunk payload/data
             *
             * @return Amount of memory used by chunk payload
             */
            public StorageUnit getTotalChunkPayloadMemory() {
                return m_totalChunkPayloadMemory;
            }

            /**
             * Get the number of currently allocated CID tables
             *
             * @return Number of CID tables
             */
            public long getCIDTableCount() {
                return m_cidTableCount;
            }

            /**
             * Get the total memory used by CID tables (payload only)
             *
             * @return Total memory used by CID tables
             */
            public StorageUnit getTotalMemoryCIDTables() {
                return m_totalMemoryCIDTables;
            }

            /**
             * Get the total amount of memory allocated and usable for actual payload/data
             *
             * @return Total amount of memory usable for payload
             */
            public StorageUnit getTotalPayloadMemory() {
                return m_totalPayloadMemory;
            }

            /**
             * Get the number of cached LIDs in the LID store
             *
             * @return Number of cached LIDs
             */
            public int getCachedFreeLIDs() {
                return m_cachedFreeLIDs;
            }

            /**
             * Get the number of total available free LIDs of the LIDStore
             *
             * @return Total number of available free LIDs
             */
            public long getAvailableFreeLIDs() {
                return m_availableFreeLIDs;
            }

            /**
             * Get the current state of the counter generating new LIDs
             *
             * @return LID counter state
             */
            public long getNewLIDCounter() {
                return m_newLIDCounter;
            }

            @Override
            public int sizeofObject() {
                return Long.BYTES * 3 + m_freeMemory.sizeofObject() + m_totalMemory.sizeofObject() + m_totalPayloadMemory.sizeofObject() +
                        m_totalChunkPayloadMemory.sizeofObject() + m_totalMemoryCIDTables.sizeofObject() + Integer.BYTES + Long.BYTES * 2;
            }

            @Override
            public void exportObject(final Exporter p_exporter) {
                p_exporter.exportObject(m_freeMemory);
                p_exporter.exportObject(m_totalMemory);
                p_exporter.exportObject(m_totalPayloadMemory);
                p_exporter.writeLong(m_numberOfActiveMemoryBlocks);
                p_exporter.writeLong(m_numberOfActiveChunks);
                p_exporter.exportObject(m_totalChunkPayloadMemory);
                p_exporter.writeLong(m_cidTableCount);
                p_exporter.exportObject(m_totalMemoryCIDTables);
                p_exporter.writeInt(m_cachedFreeLIDs);
                p_exporter.writeLong(m_availableFreeLIDs);
                p_exporter.writeLong(m_newLIDCounter);
            }

            @Override
            public void importObject(final Importer p_importer) {
                if (m_freeMemory == null) {
                    m_freeMemory = new StorageUnit();
                }
                p_importer.importObject(m_freeMemory);

                if (m_totalMemory == null) {
                    m_totalMemory = new StorageUnit();
                }
                p_importer.importObject(m_totalMemory);

                if (m_totalPayloadMemory == null) {
                    m_totalPayloadMemory = new StorageUnit();
                }
                p_importer.importObject(m_totalPayloadMemory);

                m_numberOfActiveMemoryBlocks = p_importer.readLong(m_numberOfActiveMemoryBlocks);
                m_numberOfActiveChunks = p_importer.readLong(m_numberOfActiveChunks);

                if (m_totalChunkPayloadMemory == null) {
                    m_totalChunkPayloadMemory = new StorageUnit();
                }
                p_importer.importObject(m_totalChunkPayloadMemory);

                m_cidTableCount = p_importer.readLong(m_cidTableCount);

                if (m_totalMemoryCIDTables == null) {
                    m_totalMemoryCIDTables = new StorageUnit();
                }
                p_importer.importObject(m_totalMemoryCIDTables);

                m_cachedFreeLIDs = p_importer.readInt(m_cachedFreeLIDs);
                m_availableFreeLIDs = p_importer.readLong(m_availableFreeLIDs);
                m_newLIDCounter = p_importer.readLong(m_newLIDCounter);
            }

            @Override
            public String toString() {
                String str = "";

                str += "Free memory: " + m_freeMemory.getHumanReadable() + " (" + m_freeMemory.getBytes() + ")\n";
                str += "Total memory: " + m_totalMemory.getHumanReadable() + " (" + m_totalMemory.getBytes() + ")\n";
                str += "Total payload memory: " + m_totalPayloadMemory.getHumanReadable() + " (" + m_totalPayloadMemory.getBytes() + ")\n";
                str += "Num active memory blocks: " + m_numberOfActiveMemoryBlocks + '\n';
                str += "Num active chunks: " + m_numberOfActiveChunks + '\n';
                str += "Total chunk payload memory: " + m_totalChunkPayloadMemory.getHumanReadable() + " (" + m_totalChunkPayloadMemory.getBytes() + ")\n";
                str += "Num CID tables: " + m_cidTableCount + '\n';
                str += "Total CID tables memory: " + m_totalMemoryCIDTables.getHumanReadable() + " (" + m_totalChunkPayloadMemory.getBytes() + ")\n";
                str += "Num of free LIDs cached in LIDStore: " + m_cachedFreeLIDs + '\n';
                str += "Num of total available free LIDs in LIDStore: " + m_availableFreeLIDs + '\n';
                str += "New LID counter state: " + m_newLIDCounter;
                return str;
            }
        }

    }
}
