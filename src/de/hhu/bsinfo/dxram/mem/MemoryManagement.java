package de.hhu.bsinfo.dxram.mem;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.soh.MemoryRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

import static de.hhu.bsinfo.dxram.mem.CIDTableEntry.*;

/**
 * Managing memory accesses for creating and deleting objects
 *
 * @author Florian Hucke (florian.hucke@hhu.de) on 28.02.18
 * @projectname dxram-memory
 */
public class MemoryManagement {
    private static final Logger LOGGER = LogManager.getFormatterLogger(MemoryManagement.class.getSimpleName());

    private final MemoryManager m_memManagement;
    private final SmallObjectHeap m_rawMemory;
    private final CIDTable m_cidTable;
    private final MemoryInformation m_memStats;
    private final short NODE_ID;

    private AtomicInteger m_lock;

    /**
     * Constructor
     *
     * @param memoryManager
     *          The central unit which manages all memory accesses
     *
     */
    MemoryManagement(MemoryManager memoryManager) {
        m_memManagement = memoryManager;
        m_rawMemory = memoryManager.smallObjectHeap;
        m_cidTable = memoryManager.cidTable;
        m_memStats = memoryManager.memoryInformation;

        NODE_ID = m_cidTable.m_ownNodeID;

        m_lock = new AtomicInteger(0);
    }
    
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
    long createIndex(final int p_size) throws OutOfKeyValueStoreMemoryException, MemoryRuntimeException {
        assert p_size > 0;

        long entry;
        long address;
        long size;

        long chunkID;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER createIndex p_size %d", p_size);
        // #endif /* LOGGER == TRACE */

        //get a management lock
        lockManage();
        try {
            if (m_cidTable.get(0) != 0) {
                // delete old entry
                entry = m_cidTable.delete(0, false);
                address = ADDRESS.get(entry);
                size = LENGTH_FIELD.get(entry) + 1; //+1 because of the offset

                m_rawMemory.free(address, size);
                m_memStats.totalActiveChunkMemory -= m_rawMemory.getSizeBlock(address, size);
                m_memStats.numActiveChunks--;
            }

            address = m_rawMemory.malloc(p_size);
            if (address > SmallObjectHeap.INVALID_ADDRESS) {
                //->chunkID = (long) m_boot.getNodeID() << 48;
                chunkID = (long) NODE_ID << 48; //<<

                // register new chunk in cid table
                if (!m_cidTable.set(chunkID, createEntry(address, p_size))) {
                    // on demand allocation of new table failed
                    // free previously created chunk for data to avoid memory leak
                    m_rawMemory.free(address, p_size);
                    throw new OutOfKeyValueStoreMemoryException(m_memStats.getStatus());
                } else {
                    m_memStats.numActiveChunks++;
                    m_memStats.totalActiveChunkMemory += p_size;
                }
            } else {
                throw new OutOfKeyValueStoreMemoryException(m_memStats.getStatus());
            }
        } catch (final MemoryRuntimeException e) {
            MemoryError.handleMemDumpOnError(m_rawMemory, e, ".", false, LOGGER);
            throw e;
        } finally {
            //do in any case a unlock
            unlockManage();
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT createIndex p_size %d", p_size);
        // #endif /* LOGGER == TRACE */

        return chunkID;
    }

    /**
     * Create a new chunk.
     * //TODO delete: This is a management call and has to be locked using lockManage().
     *
     * This method is Thread-Safe
     *
     * @param p_size
     *         Size in bytes of the payload the chunk contains.
     * @return Chunk ID for the allocated chunk
     */
    //TODO testing
    long create(final int p_size) throws OutOfKeyValueStoreMemoryException, MemoryRuntimeException {
        assert p_size > 0;

        long directEntryAddress;

        long address;
        long chunkID;
        long lid;

        long entry;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER create p_size %d", p_size);
        // #endif /* LOGGER == TRACE */

        lockManage();
        try {
            // #ifdef STATISTICS
            //->SOP_CREATE.enter();
            // #endif /* STATISTICS */

            // get new LID from CIDTable
            lid = m_cidTable.getFreeLID();

            // first, try to allocate. maybe early return
            // #ifdef STATISTICS
            //->SOP_MALLOC.enter(p_size);
            // #endif /* STATISTICS */
            address = m_rawMemory.malloc(p_size);
            // #ifdef STATISTICS
            //->SOP_MALLOC.leave();
            // #endif /* STATISTICS */
            if (address > SmallObjectHeap.INVALID_ADDRESS) {
                //->chunkID = ((long) m_boot.getNodeID() << 48) + lid;
                chunkID = ((long) NODE_ID << 48) + lid;//<<

                entry = createEntry(address, p_size);
                directEntryAddress = m_cidTable.getAddressOfEntryCreate(chunkID);

                // register new chunk in cid table
                if (!m_cidTable.directSet(directEntryAddress, entry)) {
                    // on demand allocation of new table failed
                    // free previously created chunk for data to avoid memory leak
                    m_rawMemory.free(address, p_size);

                    throw new OutOfKeyValueStoreMemoryException(m_memStats.getStatus());
                } else {
                    m_memStats.numActiveChunks++;
                    m_memStats.totalActiveChunkMemory += p_size;
                }
            } else {
                // put lid back
                m_cidTable.putChunkIDForReuse(lid);

                throw new OutOfKeyValueStoreMemoryException(m_memStats.getStatus());
            }

            // #ifdef STATISTICS
            //->SOP_CREATE.leave();
            // #endif /* STATISTICS */
        } catch (final MemoryRuntimeException e) {
            MemoryError.handleMemDumpOnError(m_rawMemory, e, ".", false, LOGGER);
            throw e;
        } finally {
            unlockManage();
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT create p_size %d", p_size);
        // #endif /* LOGGER == TRACE */

        return chunkID;
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
    long create(final long p_chunkId, final int p_size) throws OutOfKeyValueStoreMemoryException, MemoryRuntimeException {
        assert p_size > 0;

        long directEntryAddress;

        long address;
        long chunkID = ChunkID.INVALID_ID;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER create p_chunkId 0x%X, p_size %d", p_chunkId, p_size);
        // #endif /* LOGGER == TRACE */

        lockManage();
        try {
            // #ifdef STATISTICS
            //->SOP_CREATE.enter();
            // #endif /* STATISTICS */

            // verify this id is not used
            directEntryAddress = m_cidTable.getAddressOfEntryCreate(p_chunkId);
            long entry = m_cidTable.directGet(directEntryAddress);
            if (entry == CIDTable.ZOMBIE_ENTRY || entry == CIDTable.FREE_ENTRY) {
                address = m_rawMemory.malloc(p_size);
                if (address > SmallObjectHeap.INVALID_ADDRESS) {
                    // register new chunk
                    // register new chunk in cid table
                    if (!m_cidTable.directSet(directEntryAddress, createEntry(address, p_size))) {
                        // on demand allocation of new table failed
                        // free previously created chunk for data to avoid memory leak
                        m_rawMemory.free(address, p_size);
                        throw new OutOfKeyValueStoreMemoryException(m_memStats.getStatus());
                    } else {
                        m_memStats.numActiveChunks++;
                        m_memStats.totalActiveChunkMemory += p_size;
                        chunkID = p_chunkId;
                    }
                } else {
                    throw new OutOfKeyValueStoreMemoryException(m_memStats.getStatus());
                }
            }

            // #ifdef STATISTICS
            //->SOP_CREATE.leave();
            // #endif /* STATISTICS */
        } catch (final MemoryRuntimeException e) {
            MemoryError.handleMemDumpOnError(m_rawMemory, e, ".", false, LOGGER);
            throw e;
        } finally {
            unlockManage();
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT create p_chunkId 0x%X, p_size %d", p_chunkId, p_size);
        // #endif /* LOGGER == TRACE */

        return chunkID;
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
    long[] createMultiSizes(final int... p_sizes) {
        return createMultiSizes(false, p_sizes);
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
    //TODO testing
    long[] createMultiSizes(final boolean p_consecutive, final int... p_sizes) {
        long[] addresses;
        long[] lids;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER createMultiSizes p_consecutive %b, p_sizes %d", p_consecutive, p_sizes.length);
        // #endif /* LOGGER == TRACE */

        lockManage();
        try {
            // #ifdef STATISTICS
            //->SOP_MULTI_CREATE.enter();
            // #endif /* STATISTICS */

            // get new LIDs
            lids = m_cidTable.getFreeLIDs(p_sizes.length, p_consecutive);

            // #ifdef STATISTICS
            //->SOP_MULTI_MALLOC.enter(p_sizes.length);
            // #endif /* STATISTICS */
            addresses = m_rawMemory.multiMallocSizes(p_sizes);
            // #ifdef STATISTICS
            //->SOP_MULTI_MALLOC.leave();
            // #endif /* STATISTICS */
            if (addresses != null) {

                for (int i = 0; i < lids.length; i++) {
                    //->lids[i] = ((long) m_boot.getNodeID() << 48) + lids[i];
                    lids[i] = ((long) NODE_ID << 48) + lids[i];//<<

                    // register new chunk in cid table
                    if (!m_cidTable.setAndCreate(lids[i], createEntry(addresses[i], p_sizes[i]))) {

                        for (int j = i; j >= 0; j--) {
                            // on demand allocation of new table failed
                            // free previously created chunk for data to avoid memory leak
                            m_rawMemory.free(addresses[j], p_sizes[j]);
                        }

                        throw new OutOfKeyValueStoreMemoryException(m_memStats.getStatus());
                    } else {
                        m_memStats.numActiveChunks++;
                        m_memStats.totalActiveChunkMemory += p_sizes[i];
                    }
                }

            } else {
                // put lids back
                for (int i = 0; i < lids.length; i++) {
                    m_cidTable.putChunkIDForReuse(lids[i]);
                }

                throw new OutOfKeyValueStoreMemoryException(m_memStats.getStatus());
            }

            // #ifdef STATISTICS
            //->SOP_MULTI_CREATE.leave();
            // #endif /* STATISTICS */
        } catch (final MemoryRuntimeException e) {
            MemoryError.handleMemDumpOnError(m_rawMemory, e, ".", false, LOGGER);
            throw e;
        } finally {
            unlockManage();
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT createMultiSizes p_consecutive %b, p_sizes %d", p_consecutive, p_sizes.length);
        // #endif /* LOGGER == TRACE */

        return lids;
    }

    /**
     * Batch/Multi create with a list of data structures
     *
     * This method is Thread-Safe
     *
     * @param p_dataStructures
     *         List of data structures. Chunk ids are automatically assigned after creation
     */
    void createMulti(final DataStructure... p_dataStructures) {
        createMulti(false, p_dataStructures);
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
    //TODO testing
    void createMulti(final boolean p_consecutive, final DataStructure... p_dataStructures) {
        int[] sizes = new int[p_dataStructures.length];

        for (int i = 0; i < p_dataStructures.length; i++) {
            sizes[i] = p_dataStructures[i].sizeofObject();
        }

        long[] ids = createMultiSizes(p_consecutive, sizes);

        for (int i = 0; i < ids.length; i++) {
            p_dataStructures[i].setID(ids[i]);
        }
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
    long[] createMulti(final int p_size, final int p_count) {
        return createMulti(p_size, p_count, false);
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
    //TODO testing
    long[] createMulti(final int p_size, final int p_count, final boolean p_consecutive) {
        long[] addresses;
        long[] lids;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER createMultiSizes p_size %d, p_count %d, p_consecutive %b", p_size, p_count, p_consecutive);
        // #endif /* LOGGER == TRACE */

        lockManage();
        try {
            // #ifdef STATISTICS
            //->SOP_MULTI_CREATE.enter();
            // #endif /* STATISTICS */

            // get new LIDs
            lids = m_cidTable.getFreeLIDs(p_count, p_consecutive);

            // first, try to allocate. maybe early return
            // #ifdef STATISTICS
            //->SOP_MULTI_MALLOC.enter(p_size);
            // #endif /* STATISTICS */
            addresses = m_rawMemory.multiMalloc(p_size, p_count);
            // #ifdef STATISTICS
            //->SOP_MULTI_MALLOC.leave();
            // #endif /* STATISTICS */
            if (addresses != null) {

                for (int i = 0; i < lids.length; i++) {
                    //->lids[i] = ((long) m_boot.getNodeID() << 48) + lids[i];
                    lids[i] = ((long) NODE_ID << 48) + lids[i];//<<

                    // register new chunk in cid table
                    if (!m_cidTable.setAndCreate(lids[i], createEntry(addresses[i], p_size))) {

                        for (int j = i; j >= 0; j--) {
                            // on demand allocation of new table failed
                            // free previously created chunk for data to avoid memory leak
                            m_rawMemory.free(addresses[j], p_size);
                        }

                        throw new OutOfKeyValueStoreMemoryException(m_memStats.getStatus());
                    } else {
                        m_memStats.numActiveChunks++;
                        m_memStats.totalActiveChunkMemory += p_size;
                    }
                }

            } else {
                // put lids back
                for (int i = 0; i < lids.length; i++) {
                    m_cidTable.putChunkIDForReuse(lids[i]);
                }

                throw new OutOfKeyValueStoreMemoryException(m_memStats.getStatus());
            }

            // #ifdef STATISTICS
            //->SOP_MULTI_CREATE.leave();
            // #endif /* STATISTICS */
        } catch (final MemoryRuntimeException e) {
            MemoryError.handleMemDumpOnError(m_rawMemory, e, ".", false, LOGGER);
            throw e;
        } finally {
            unlockManage();
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT createMultiSizes p_size %d, p_count %d, p_consecutive %b", p_size, p_count, p_consecutive);
        // #endif /* LOGGER == TRACE */

        return lids;
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
    //TODO testing locks
    void createAndPutRecovered(final long[] p_chunkIDs, final byte[] p_data, final int[] p_offsets, final int[] p_lengths, final int p_usedEntries) {
        long[] addresses;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER createAndPutRecovered, count %d", p_chunkIDs.length);
        // #endif /* LOGGER == TRACE */

        try {
            // #ifdef STATISTICS
            //->SOP_CREATE_PUT_RECOVERED.enter(p_usedEntries);
            // #endif /* STATISTICS */

            // #ifdef STATISTICS
            //->SOP_MULTI_MALLOC.enter(p_usedEntries);
            // #endif /* STATISTICS */
            addresses = m_rawMemory.multiMallocSizesUsedEntries(p_usedEntries, p_lengths);
            // #ifdef STATISTICS
            //->SOP_MULTI_MALLOC.leave();
            // #endif /* STATISTICS */
            if (addresses != null) {

                for (int i = 0; i < addresses.length; i++) {
                    m_rawMemory.writeBytes(addresses[i], 0, p_data, p_offsets[i], p_lengths[i],
                            (p_lengths[i]-1) & (LENGTH_FIELD.BITMASK >> LENGTH_FIELD.OFFSET));
                    m_memStats.totalActiveChunkMemory += p_lengths[i];
                }

                m_memStats.numActiveChunks += addresses.length;

                for (int i = 0; i < addresses.length; i++) {
                    m_cidTable.setAndCreate(p_chunkIDs[i], createEntry(addresses[i], p_lengths[i]));
                }
            } else {
                throw new OutOfKeyValueStoreMemoryException(m_memStats.getStatus());
            }

            // #ifdef STATISTICS
            //->SOP_CREATE_PUT_RECOVERED.leave();
            // #endif /* STATISTICS */
        } catch (final MemoryRuntimeException e) {
            MemoryError.handleMemDumpOnError(m_rawMemory, e, ".", false, LOGGER);
            throw e;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("EXIT createAndPutRecovered, count %d", p_chunkIDs.length);
        // #endif /* LOGGER == TRACE */
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
    int createAndPutRecovered(final DataStructure... p_dataStructures) {
        int ret = 0;
        long[] addresses;
        int[] sizes = new int[p_dataStructures.length];

        for (int i = 0; i < p_dataStructures.length; i++) {
            sizes[i] = p_dataStructures[i].sizeofObject();
        }

        try {
            // #ifdef STATISTICS
            //->SOP_CREATE_PUT_RECOVERED.enter(p_dataStructures.length);
            // #endif /* STATISTICS */

            // #ifdef STATISTICS
            //->SOP_MULTI_MALLOC.enter(p_dataStructures.length);
            // #endif /* STATISTICS */
            addresses = m_rawMemory.multiMallocSizes(sizes);
            // #ifdef STATISTICS
            //->SOP_MULTI_MALLOC.leave();
            // #endif /* STATISTICS */
            if (addresses != null) {

                for (int i = 0; i < addresses.length; i++) {
                    SmallObjectHeapDataStructureImExporter exporter = m_memManagement.getImExporter(addresses[i],
                            sizes[i] & (LENGTH_FIELD.BITMASK >> LENGTH_FIELD.OFFSET));
                    exporter.exportObject(p_dataStructures[i]);
                    ret += sizes[i];
                    m_memStats.totalActiveChunkMemory += sizes[i];
                }

                m_memStats.numActiveChunks += addresses.length;

                for (int i = 0; i < addresses.length; i++) {
                    m_cidTable.set(p_dataStructures[i].getID(), createEntry(addresses[i], sizes[i]));
                }
            } else {
                throw new OutOfKeyValueStoreMemoryException(m_memStats.getStatus());
            }

            // #ifdef STATISTICS
            //->SOP_CREATE_PUT_RECOVERED.leave();
            // #endif /* STATISTICS */
        } catch (final MemoryRuntimeException e) {
            MemoryError.handleMemDumpOnError(m_rawMemory, e, ".", false, LOGGER);
            throw e;
        }

        return ret;
    }

    /**
     * Removes a Chunk from the memory
     * This method use a switchable lock. //TODO Reset to normal lock
     *
     * This operation is Thread-Safe
     *
     * @param p_chunkID
     *         the ChunkID of the Chunk
     * @param p_wasMigrated
     *         default value for this parameter should be false!
     *         if chunk was deleted during migration this flag should be set to true
     * @return The size of the deleted chunk if removing the data was successful, -1 if the chunk with the specified id does not exist
     */
    int remove(final long p_chunkID, final boolean p_wasMigrated) {
        int ret = -1;

        long directEntryAddress;
        long entry;
        long addressDeletedChunk;
        long size;
        boolean deleted;

        // #if LOGGER == TRACE
        LOGGER.trace("ENTER remove p_chunkID 0x%X, p_wasMigrated %d", p_chunkID, p_wasMigrated);
        // #endif /* LOGGER == TRACE */

        if (p_chunkID != ChunkID.INVALID_ID &&
                (directEntryAddress = m_cidTable.getAddressOfEntry(p_chunkID)) != SmallObjectHeap.INVALID_ADDRESS &&
                m_memManagement.switchableWriteLock(directEntryAddress)) {
            lockManage();
            try {
                // #ifdef STATISTICS
                //->SOP_REMOVE.enter();
                // #endif /* STATISTICS */

                // Get and delete the address from the CIDTable, mark as zombie first
                entry = m_cidTable.directGet(directEntryAddress);
                if(STATE_NOT_MOVEABLE.get(entry) || STATE_NOT_REMOVEABLE.get(entry)){
                    unlockManage();
                    LOGGER.info("CID: %d is not remove able!!!!", p_chunkID);
                    return 0;
                }
                deleted = FULL_FLAG.get(entry) || entry == CIDTable.FREE_ENTRY;
                if(deleted)
                    return -1;

                m_cidTable.directSet(directEntryAddress, CIDTable.ZOMBIE_ENTRY);
                addressDeletedChunk = ADDRESS.get(entry);
                size = LENGTH_FIELD.get(entry) + 1; //+1 because of the offset

                if (addressDeletedChunk > SmallObjectHeap.INVALID_ADDRESS) {

                    if (p_wasMigrated) {
                        // deleted and previously migrated chunks don't end up in the LID store
                        m_cidTable.directSet(directEntryAddress, CIDTable.FREE_ENTRY);
                    } else {
                        // more space for another zombie for reuse in LID store?
                        if (m_cidTable.putChunkIDForReuse(ChunkID.getLocalID(p_chunkID))) {
                            // kill zombie entry
                            m_cidTable.directSet(directEntryAddress, CIDTable.FREE_ENTRY);
                        } else {
                            // no space for zombie in LID store, keep him "alive" in table
                        }
                    }
                    ret = m_rawMemory.getSizeBlock(addressDeletedChunk, size);
                    // #ifdef STATISTICS
                    //->SOP_FREE.enter(ret);
                    // #endif /* STATISTICS */
                    m_rawMemory.free(addressDeletedChunk, size);
                    // #ifdef STATISTICS
                    //->SOP_FREE.leave();
                    // #endif /* STATISTICS */
                    m_memStats.numActiveChunks--;
                    m_memStats.totalActiveChunkMemory -= ret;
                }

            } catch (final MemoryRuntimeException e) {
                //handleMemDumpOnError(e, false);
                throw e;
            } finally {
                m_memManagement.switchableWriteUnlock(directEntryAddress);
                unlockManage();
            }
        }


        // #if LOGGER == TRACE
        LOGGER.trace("EXIT remove p_chunkID 0x%X, p_wasMigrated %d", p_chunkID, p_wasMigrated);
        // #endif /* LOGGER == TRACE */

        return ret;
    }

    /**
     * Lock the memory for a management task (create, put, remove).
     */
    private void lockManage() {
        //m_lock.writeLock().lock();

        do {
            int v = m_lock.get();
            m_lock.compareAndSet(v, v | 0x40000000);
        } while (!m_lock.compareAndSet(0x40000000, 0x80000000));
    }

    /**
     * Unlock the memory after a management task (create, put, remove).
     */
    private void unlockManage() {
        //m_lock.writeLock().unlock();

        m_lock.set(0);
    }
}
