package de.hhu.bsinfo.dxram.mem;

import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.mem.manipulation.ByteDataManipulation;
import de.hhu.bsinfo.soh.MemoryRuntimeException;
import de.hhu.bsinfo.soh.StorageUnsafeMemory;

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
    final MemoryAccess memoryAccess;
    final MemoryDirectAccess memoryDirectAccess;
    final MemoryInformation memoryInformation;
    final MemoryManagement memoryManagement;
    final MemoryPinning memoryPinning;

    private boolean m_readLock = true;
    private boolean m_writeLock = true;

    public MemoryManager(final short p_nodeID, final long p_heapSize, final int p_maxBlockSize) {
        smallObjectHeap = new SmallObjectHeap(new StorageUnsafeMemory(), p_heapSize, p_maxBlockSize);
        cidTable = new CIDTable(p_nodeID);
        cidTable.initialize(smallObjectHeap);
        memoryAccess = new MemoryAccess(this);
        memoryDirectAccess = new MemoryDirectAccess(this);
        memoryInformation = new MemoryInformation(this);
        memoryManagement = new MemoryManagement(this);
        memoryPinning = new MemoryPinning(this);

        memoryInformation.numActiveChunks = 0;
        memoryInformation.totalActiveChunkMemory = 0;
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
        return memoryAccess.get(p_dataStructure);
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
        return memoryAccess.get(p_chunkID);
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
        return memoryAccess.get(p_chunkID, p_buffer, p_offset, p_bufferSize);
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
        return memoryAccess.put(p_dataStructure);
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
        return memoryAccess.put(p_chunkID, p_data);
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
        return memoryAccess.put(p_chunkID, p_data, p_offset, p_length);
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
        return memoryAccess.modify(p_chunkID, byteDataManipulation);
    }

    //MemoryPinning
    /**
     * Pin a chunk for direct access
     *
     * @param chunkID Chunk ID to pin
     * @return The CIDTable entry
     */
    public long pinChunk(final long chunkID){
        return memoryPinning.pinChunk(chunkID);
    }

    /**
     * Unpin a Chunks (slow operation need a DFS over all CIDTable tables)
     *
     * @param cidTableEntry CIDTable entry.
     * @return The corresponding chunk ID or -1 if no suitable chunk ID was found
     */
    public long unpinChunk(final long cidTableEntry){
        return memoryPinning.unpinChunk(cidTableEntry);
    }

    /**
     * Get the data for a entry
     *
     * @param cidTableEntry CIDTable entry
     * @return Chunk data
     */
    public byte[] getPinned(final long cidTableEntry){
        return memoryPinning.get(cidTableEntry);
    }

    /**
     * Put data to a entry
     *
     * @param cidTableEntry Entry
     * @param data Data to put
     */
    public void putPinned(final long cidTableEntry, final byte[] data){
        memoryPinning.put(cidTableEntry, data);
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
        return memoryManagement.createIndex(p_size);
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
        return memoryManagement.create(p_size);
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
        return memoryManagement.create(p_chunkId, p_size);
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
        return memoryManagement.createMultiSizes(p_sizes);
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
        return memoryManagement.createMultiSizes(p_consecutive, p_sizes);
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
        memoryManagement.createMulti(p_dataStructures);
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
        memoryManagement.createMulti(p_consecutive, p_dataStructures);
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
        return memoryManagement.createMulti(p_size, p_count);
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
        return memoryManagement.createMulti(p_size, p_count, p_consecutive);
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
        memoryManagement.createAndPutRecovered(p_chunkIDs, p_data, p_offsets, p_lengths, p_usedEntries);
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
        return memoryManagement.createAndPutRecovered(p_dataStructures);
    }

    public int remove(final long p_chunkID, final boolean p_wasMigrated) {
        return memoryManagement.remove(p_chunkID, p_wasMigrated);
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
     *          True if a lock is received, else false
     *
     */
    boolean switchableReadLock(final long p_directEntryAddress){
        if(m_readLock)
            return cidTable.directReadLock( p_directEntryAddress);
        else
            return cidTable.directWriteLock( p_directEntryAddress);
    }

    /**
     * Read unlock, this unlock is switchable, with the method
     * setLocks.
     *
     *
     * @param  p_directEntryAddress
     *              Position of the CIDTable entry
     * @return
     *          True if a unlock was successful, else false
     *
     */
    boolean switchableReadUnlock(final long p_directEntryAddress){
        if(m_readLock)
            return cidTable.directReadUnlock( p_directEntryAddress);
        else
            return cidTable.directWriteUnlock( p_directEntryAddress);
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
}
