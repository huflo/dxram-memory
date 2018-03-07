/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.mem;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.ChunkIDRanges;
import de.hhu.bsinfo.utils.ArrayListLong;
import de.hhu.bsinfo.utils.BitMask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static de.hhu.bsinfo.dxram.mem.CIDTableEntry.*;

//import de.hhu.bsinfo.utils.stats.StatisticsOperation;
//import de.hhu.bsinfo.utils.stats.StatisticsRecorderManager;

/**
 * Paging-like Tables for the ChunkID-VA mapping
 *
 * @author Florian Klein, florian.klein@hhu.de, 13.02.2014
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.11.2015
 * @author Florian Hucke, florian.hucke@hhu.de, 06.02.2018
 */
@SuppressWarnings("unused")
final class CIDTable {
    private static final byte ENTRY_SIZE = 8;
    static final byte LID_TABLE_LEVELS = 4;

    static final long FREE_ENTRY = 0;
    static final long ZOMBIE_ENTRY = BitMask.createMask(64, 0);
    private static final Logger LOGGER = LogManager.getFormatterLogger(CIDTable.class.getSimpleName());
    // statistics recorder
    //private static final StatisticsOperation SOP_CREATE_NID_TABLE = StatisticsRecorderManager.getOperation(MemoryManagerComponent.class, "CreateNIDTable");
    //private static final StatisticsOperation SOP_CREATE_LID_TABLE = StatisticsRecorderManager.getOperation(MemoryManagerComponent.class, "CreateLIDTable");
    private static final byte BITS_PER_LID_LEVEL = 48 / LID_TABLE_LEVELS;
    static final int ENTRIES_PER_LID_LEVEL = (int) Math.pow(2.0, BITS_PER_LID_LEVEL);
    static final int LID_TABLE_SIZE = ENTRY_SIZE * ENTRIES_PER_LID_LEVEL + 7;
    private static final long LID_LEVEL_BITMASK = (int) Math.pow(2.0, BITS_PER_LID_LEVEL) - 1;
    private static final byte BITS_FOR_NID_LEVEL = 16;
    static final int ENTRIES_FOR_NID_LEVEL = (int) Math.pow(2.0, BITS_FOR_NID_LEVEL);
    static final int NID_TABLE_SIZE = ENTRY_SIZE * ENTRIES_FOR_NID_LEVEL + 7;
    private static final long NID_LEVEL_BITMASK = (int) Math.pow(2.0, BITS_FOR_NID_LEVEL) - 1;

    short m_ownNodeID;
    private long m_addressTableDirectory = -1;
    private SmallObjectHeap m_rawMemory;
    private int m_tableCount = -1;
    private long m_totalMemoryTables = -1;

    private LIDStore m_store;
    private long m_nextLocalID;

    private TranslationCache[] m_cache;

    /**
     * Creates an instance of CIDTable
     *
     * @param p_ownNodeID
     *     Own node ID
     */
    CIDTable(final short p_ownNodeID) {
        m_ownNodeID = p_ownNodeID;
    }

    /**
     * Get the number of tables currently allocated.
     *
     * @return Number of tables currently allocated.
     */
    int getTableCount() {
        return m_tableCount;
    }

    /**
     * Get the total amount of memory used by the tables.
     *
     * @return Amount of memory used by the tables (in bytes)
     */
    long getTotalMemoryTables() {
        return m_totalMemoryTables;
    }

    /**
     * Get the number of cached free LIDs of the LIDStore
     *
     * @return Number of cached free LIDs
     */
    int getNumCachedFreeLIDs() {
        return m_store.m_count;
    }

    /**
     * Get the number of total available free LIDs of the LIDStore
     *
     * @return Number of total available free LIDs
     */
    long getNumAvailableFreeLIDs() {
        return m_store.m_overallCount;
    }

    /**
     * Get the current state of the counter generating new LIDs
     *
     * @return LID counter state
     */
    long getNextLocalIDCounter() {
        return m_nextLocalID;
    }

    /**
     * Get a free LID from the CIDTable
     *
     * @return a free LID and version
     */
    long getFreeLID() {
        long ret;

        ret = m_store.get();

        // If no free ID exist, get next local ID
        if (ret == -1) {
            ret = m_nextLocalID++;
            // as 63-bit counter is enough for now and a while, so we don't check for overflows
        }

        return ret;
    }

    /**
     * Get a free LID from the CIDTable
     *
     * @return a free LID and version
     */
    long[] getFreeLIDs(final int p_size, final boolean p_consecutive) {
        long[] ret;

        if (!p_consecutive) {
            ret = new long[p_size];
            for (int i = 0; i < p_size; i++) {
                ret[i] = m_store.get();

                // If no free ID exist, get next local ID
                if (ret[i] == -1) {
                    ret[i] = m_nextLocalID++;
                }

                // as 63-bit counter is enough for now and a while, so we don't check for overflows
            }
        } else {
            ret = m_store.getConsecutiveLIDs(p_size);
            if (ret == null) {
                // There are not enough consecutive entries in LIDStore
                ret = new long[p_size];
                for (int i = 0; i < p_size; i++) {
                    ret[i] = m_nextLocalID++;

                    // as 63-bit counter is enough for now and a while, so we don't check for overflows
                }
            }
        }

        return ret;
    }

    /**
     * Returns the ChunkID ranges of all locally stored Chunks
     *
     * @return the ChunkID ranges
     */
    ChunkIDRanges getCIDRangesOfAllLocalChunks() {
        ArrayListLong ret;
        long entry;

        ret = new ArrayListLong();
        for (int i = 0; i < ENTRIES_FOR_NID_LEVEL; i++) {
            entry = ADDRESS.get(readEntry(m_addressTableDirectory, i,NID_TABLE_SIZE));
            if (entry > 0) {
                if (i == (m_ownNodeID & 0xFFFF)) {
                    getAllRanges(ret, (long) i << 48,
                            ADDRESS.get(readEntry(m_addressTableDirectory, i & NID_LEVEL_BITMASK, NID_TABLE_SIZE)),
                            LID_TABLE_LEVELS - 1);
                }
            }
        }

        return ChunkIDRanges.wrap(ret);
    }

    /**
     * Returns the ChunkID ranges of all migrated Chunks
     *
     * @return the ChunkID ranges of all migrated Chunks
     */
    ChunkIDRanges getCIDRangesOfAllMigratedChunks() {
        ArrayListLong ret;
        long entry;

        ret = new ArrayListLong();
        for (int i = 0; i < ENTRIES_FOR_NID_LEVEL; i++) {
            entry = ADDRESS.get(readEntry(m_addressTableDirectory, i, NID_TABLE_SIZE));
            if (entry > 0 && i != (m_ownNodeID & 0xFFFF)) {
                getAllRanges(ret, (long) i << 48,
                        ADDRESS.get(readEntry(m_addressTableDirectory, i & NID_LEVEL_BITMASK, NID_TABLE_SIZE)),
                        LID_TABLE_LEVELS - 1);
            }
        }

        return ChunkIDRanges.wrap(ret);
    }

    /**
     * Initializes the CIDTable
     *
     * @param p_rawMemory
     *     The raw memory instance to use for allocation.
     */
    void initialize(final SmallObjectHeap p_rawMemory) {
        m_rawMemory = p_rawMemory;
        m_tableCount = 0;
        m_totalMemoryTables = 0;
        m_addressTableDirectory = createNIDTable();

        m_store = new LIDStore();
        m_nextLocalID = 1;

        // NOTE: 10 seems to be a good value because it doesn't add too much overhead when creating huge ranges of chunks
        // but still allows 10 * 4096 translations to be cached for fast lookup and gets/puts
        // (value determined by profiling the application)
        m_cache = new TranslationCache[10000];
        for (int i = 0; i < m_cache.length; i++) {
            m_cache[i] = new TranslationCache(10);
        }

        // #if LOGGER >= INFO
        LOGGER.info("CIDTable: init success (page directory at: 0x%X)", m_addressTableDirectory);
        // #endif /* LOGGER >= INFO */
    }

    /**
     * Gets an entry of the level 0 table
     *
     * @param p_chunkID
     *     the ChunkID of the entry
     * @return the entry. 0 for invalid/unused.
     */
    long get(final long p_chunkID) {
        return get(getAddressOfEntry(p_chunkID));

    }

    /**
     * Gets an entry of the level 0 table
     *
     * @param p_entryPosition
     *     address of the level 0 table with offset
     * @return the entry. 0 for invalid/unused.
     */
    long get(final long[] p_entryPosition){
        if(p_entryPosition == null){
            return 0;
        } else {
            return readEntry(p_entryPosition[0], p_entryPosition[1], LID_TABLE_SIZE);
        }
    }

    /**
     * Sets an entry of the level 0 table
     *
     * @param p_entryPosition
     *     address of the level 0 table with offset
     * @param p_chunkEntry
     *     the coded entry of the chunk with the address, a 10 bit space of a length field and different states
     * @return True if successful, false if allocation of a new table failed, out of memory
     */
    boolean set(final long[] p_entryPosition, final long p_chunkEntry){
        if(p_entryPosition == null || p_entryPosition.length != 2)
            return false;
        else {
            writeEntry(p_entryPosition[0], p_entryPosition[1], p_chunkEntry, LID_TABLE_SIZE);
            return true;
        }
    }

    /**
     * Sets an entry of the level 0 table
     *
     * @param p_chunkID
     *     the ChunkID of the entry
     * @param p_chunkEntry
     *     the coded entry of the chunk with the address, a 10 bit space of a length field and different states
     * @return True if successful, false if allocation of a new table failed, out of memory
     */
    boolean set(final long p_chunkID, final long p_chunkEntry) {
        return set(getAddressOfEntry(p_chunkID), p_chunkEntry);
    }

    boolean setAndCreate(final long p_chunkID, final long p_chunkEntry){
        return set(getAddressOfEntryCreate(p_chunkID), p_chunkEntry);
    }

    /**
     * Gets and deletes an entry of the level 0 table
     *
     * @param p_chunkID
     *     the ChunkID of the entry
     * @param p_flagZombie
     *     Flag the deleted entry as a zombie or not zombie i.e. fully deleted.
     * @return The entry of the chunk which was removed from the table.
     */
    long delete(final long p_chunkID, final boolean p_flagZombie) {
        long ret = -1;
        long index;
        long entry;
        long tableSize = NID_TABLE_SIZE;

        int level = 4;
        long addressTable = m_addressTableDirectory;

        //caching is not effective here. If a level 0 table in the level 1 table is marked as full this marker is kept.

        // try to jump to table level 0 using the cache
        //addressTable = m_cache[(int) Thread.currentThread().getId()].getTableLevel0(p_chunkID);
        //if (addressTable == -1) {
        //    level = LID_TABLE_LEVELS;
        //    addressTable = m_addressTableDirectory;
        //}

        do {
            if (level == LID_TABLE_LEVELS) {
                index = p_chunkID >> BITS_PER_LID_LEVEL * level & NID_LEVEL_BITMASK;
            } else {
                index = p_chunkID >> BITS_PER_LID_LEVEL * level & LID_LEVEL_BITMASK;
                tableSize = LID_TABLE_SIZE;
            }

            if (level > 0) {
                // Read table entry
                entry = readEntry(addressTable, index, tableSize);

                //We don't delete Table so only on level 1 we can have a change
                if (FULL_FLAG.get(entry) && level == 1) {
                    // Delete full flag
                    entry = FULL_FLAG.set(entry, false);
                    writeEntry(addressTable, index, entry, tableSize);
                }

                if (ADDRESS.get(entry) == 0) {
                    break;
                }

                // Delete entry in the following table
                addressTable = ADDRESS.get(entry);
            } else {
                ret = readEntry(addressTable, index, tableSize);

                // already deleted
                if (ret == FREE_ENTRY || ret == ZOMBIE_ENTRY) {
                    return 0;
                }

                // Delete the level 0 entry
                // invalid + active address but deleted
                // -> zombie entry
                if (p_flagZombie) {
                    writeEntry(addressTable, index, ZOMBIE_ENTRY, tableSize);
                } else {
                    writeEntry(addressTable, index, FREE_ENTRY, tableSize);
                }
            }

            level--;
        } while (level >= 0);

        return ret;
    }

    /**
     * Get the table address and the index of a CID
     *
     * @param p_chunkID the CID we want to know the memory address
     * @return a long array with the table address and the index of the entry or null if there is no suitable CID entry
     */
    long[] getAddressOfEntry(long p_chunkID){
        long index;
        long entry;

        int level = 0;
        long addressTable;
        boolean putCache = false;

        // try to jump to table level 0 using the cache
        addressTable = m_cache[(int) Thread.currentThread().getId()].getTableLevel0(p_chunkID);
        if (addressTable == -1) {
            level = LID_TABLE_LEVELS;
            addressTable = m_addressTableDirectory;
            putCache = true;
        }

        do {
            if (level == LID_TABLE_LEVELS) {
                index = p_chunkID >> BITS_PER_LID_LEVEL * level & NID_LEVEL_BITMASK;
            } else {
                index = p_chunkID >> BITS_PER_LID_LEVEL * level & LID_LEVEL_BITMASK;
            }

            if (level > 0) {
                entry = readEntry(addressTable, index, LID_TABLE_SIZE);

                if (entry == ZOMBIE_ENTRY || entry == FREE_ENTRY) {
                    break;
                }

                // move on to next table
                addressTable = ADDRESS.get(entry);
            } else {
                // add table 0 address to cache
                if (putCache) {
                    m_cache[(int) Thread.currentThread().getId()].putTableLevel0(p_chunkID, addressTable);
                }

                // get address of the entry
                return new long[]{addressTable, index};
            }

            level--;
        } while (level >= 0);

        return null;
    }

    /**
     * Get the table address and the index of a CID. If a table does not exist, create it.
     *
     * @param p_chunkID the CID we want to know the memory address
     * @return a long array with the table address and the index of the entry or null if there is no suitable CID entry
     */
    long[] getAddressOfEntryCreate(long p_chunkID){
        long index;
        long entry;

        int level = 0;
        long addressTable;
        boolean putCache = false;

        // try to jump to table level 0 using the cache
        addressTable = m_cache[(int) Thread.currentThread().getId()].getTableLevel0(p_chunkID);
        if (addressTable == -1) {
            level = LID_TABLE_LEVELS;
            addressTable = m_addressTableDirectory;
            putCache = true;
        }

        do {
            if (level == LID_TABLE_LEVELS) {
                index = p_chunkID >> BITS_PER_LID_LEVEL * level & NID_LEVEL_BITMASK;
            } else {
                index = p_chunkID >> BITS_PER_LID_LEVEL * level & LID_LEVEL_BITMASK;
            }

            if (level > 0) {

                entry = readEntry(addressTable, index, LID_TABLE_SIZE);

                if (entry == 0) {
                    entry = createLIDTable();
                    if (entry == SmallObjectHeap.INVALID_ADDRESS) {
                        return null;
                    }
                    writeEntry(addressTable, index, entry, LID_TABLE_SIZE);
                }

                // move on to next table
                addressTable = ADDRESS.get(entry);
            } else {
                // add table 0 address to cache
                if (putCache) {
                    m_cache[(int) Thread.currentThread().getId()].putTableLevel0(p_chunkID, addressTable);
                }

                // get address of the entry
                return new long[]{addressTable, index};
            }

            level--;
        } while (level >= 0);

        return null;
    }

    /**
     * Disengages the CIDTable
     */
    void disengage() {
        m_store = null;

        m_addressTableDirectory = -1;
    }

    // -----------------------------------------------------------------------------------------

    /**
     * Puts the LocalID of a deleted migrated Chunk to LIDStore
     *
     * @param p_chunkID
     *     the ChunkID of the entry
     * @return m_cidTable
     */
    boolean putChunkIDForReuse(final long p_chunkID) {
        return m_store.put(ChunkID.getLocalID(p_chunkID));
    }

    /**
     * Reads a table entry
     *
     * (Need to be package-private because of the analyzer)
     *
     * @param p_addressTable
     *     the table
     * @param p_index
     *     the index of the entry
     * @param p_tableSize
     *     the size of the table
     * @return the entry
     */
    long readEntry(final long p_addressTable, final long p_index, final long p_tableSize) {
        return m_rawMemory.readLongRaw(p_addressTable, ENTRY_SIZE * p_index, p_tableSize);
    }

    /**
     * Writes a table entry
     *
     * @param p_addressTable
     *     the table
     * @param p_index
     *     the index of the entry
     * @param p_entry
     *     the entry
     * @param p_tableSize
     *     the size of the table
     */
    private void writeEntry(final long p_addressTable, final long p_index, final long p_entry, final long p_tableSize) {
        m_rawMemory.writeLongRaw(p_addressTable, ENTRY_SIZE * p_index, p_entry, p_tableSize);
    }

    /**
     * Get a read lock on a CID
     *
     * @param p_chunkID the cid we want to lock
     * @return False if the chunk is no longer active. True on success.
     */
    final boolean readLock(final long p_chunkID) {
        long[] entry;

        return (entry = getAddressOfEntry(p_chunkID)) != null && readLock(entry);
    }

    /**
     * Get a read lock on a CID
     *
     * @param p_entryPosition
     *          An array of size 2 with the table address and the index in the table
     * @return False if the chunk is no longer active. True on success.
     */
    final boolean readLock(final long[] p_entryPosition) {
        assert p_entryPosition.length == 2;

        return readLock(p_entryPosition[0], p_entryPosition[1]);
    }

    /**
     * Get a read lock on a index in a table
     *
     * @param p_tableAddress address of the level 0 table
     * @param p_index row in the table
     * @return False if the chunk is no longer active. True on success.
     */
    private boolean readLock(final long p_tableAddress, final long p_index){
        long m_offset = p_index * ENTRY_SIZE;
        long value;

        while(true) {
            value = m_rawMemory.readLongRaw(p_tableAddress, m_offset, LID_TABLE_SIZE);

            //check if entry is alive
            if(value == FREE_ENTRY || value == ZOMBIE_ENTRY)
                return false;

            //for evalutation do three tries
            //1. with Thread.yield()
            //2. with LockSupport.parkNanos(long)
            //3. no Thread Handle
            if ((value & READ_ACCESS.BITMASK) == READ_ACCESS.BITMASK ||
                    (value & WRITE_ACCESS.BITMASK) == WRITE_ACCESS.BITMASK){
                //Thread.yield();
                continue;
            }

            if (m_rawMemory.compareAndSwapLong(p_tableAddress, m_offset, value, value + READ_INCREMENT))
                break;

            //Thread.yield();
        }

        return true;
    }

    /**
     * Release a read lock on a CID
     *
     * @param p_chunkID the ID of the chunk
     * @return False if there was no lock or the chunk is no longer active. True on success.
     */
    final boolean readUnlock(final long p_chunkID) {
        long[] entry;

        return (entry = getAddressOfEntry(p_chunkID)) != null &&
                readUnlock(entry);
    }

    /**
     * Release a read lock on a CID
     *
     * @param p_entryPosition
     *          An array of size 2 with the table address and the index in the table
     * @return False if the chunk is no longer active. True on success.
     */
    final boolean readUnlock(final long[] p_entryPosition) {
        assert p_entryPosition.length == 2;

        return readUnlock(p_entryPosition[0], p_entryPosition[1]);
    }

    /**
     * Release a read lock on a index in a table
     *
     * @param p_tableAddress address of the level 0 table
     * @param p_index row in the table
     * @return False if there was no lock or the chunk is no longer active. True on success.
     */
    private boolean readUnlock(final long p_tableAddress, final long p_index){
        long m_offset = p_index * ENTRY_SIZE;
        long value;

        while(true){
            value = m_rawMemory.readLongRaw(p_tableAddress, m_offset, LID_TABLE_SIZE);

            //check if entry is alive
            if(value == FREE_ENTRY || value == ZOMBIE_ENTRY)
                return false;

            //no read lock is set
            if((value & READ_ACCESS.BITMASK) == 0)
                return false;

            if(m_rawMemory.compareAndSwapLong(p_tableAddress, m_offset, value, value - READ_INCREMENT))
                break;

            //Thread.yield();

        }
        return true;
    }

    /**
     * Get a write lock on a CID
     *
     * @param p_chunkID the cid we want to lock
     * @return False if the chunk is no longer active. True on success.
     */
    final boolean writeLock(final long p_chunkID) {
        long[] entry;

        return (entry = getAddressOfEntry(p_chunkID)) != null && writeLock(entry);
    }

    /**
     * Get a write lock on a CID
     *
     * @param p_entryPosition
     *          An array of size 2 with the table address and the index in the table
     * @return False if the chunk is no longer active. True on success.
     */
    final boolean writeLock(final long[] p_entryPosition) {
        assert p_entryPosition.length == 2;

        return writeLock(p_entryPosition[0], p_entryPosition[1]);
    }

    /**
     * Get a write lock on a index in a table
     *
     * @param p_tableAddress address of the level 0 table
     * @param p_index row in the table
     * @return False if the chunk is no longer active. True on success.
     */
    private boolean writeLock(final long p_tableAddress, final long p_index){
        long m_offset = p_index * ENTRY_SIZE;
        long value;

        while(true){
            value = m_rawMemory.readLongRaw(p_tableAddress, m_offset, LID_TABLE_SIZE);

            //check if entry is alive
            if(value == FREE_ENTRY || value == ZOMBIE_ENTRY)
                return false;

            if((value & WRITE_ACCESS.BITMASK) == WRITE_ACCESS.BITMASK){
                Thread.yield();
                continue;
            }

            if(m_rawMemory.compareAndSwapLong(p_tableAddress, m_offset, value, value | WRITE_ACCESS.BITMASK))
                break;

        }

        // wait until no present read access
        while((m_rawMemory.readLongRaw(p_tableAddress, m_offset, LID_TABLE_SIZE) & READ_ACCESS.BITMASK) != 0){
            Thread.yield();
        }

        return true;
    }

    /**
     * Release a read lock on a CID
     *
     * @param p_chunkID the ID of the chunk
     * @return False if there was no lock or the chunk is no longer active. True on success.
     */
    final boolean writeUnlock(final long p_chunkID) {

        long[] entry;
        return (entry = getAddressOfEntry(p_chunkID)) != null && writeUnlock(entry);
    }

    /**
     * Release a write lock on a CID
     *
     * @param p_entryPosition
     *          An array of size 2 with the table address and the index in the table
     * @return False if the chunk is no longer active. True on success.
     */
    final boolean writeUnlock(final long[] p_entryPosition) {
        assert p_entryPosition.length == 2;

        return writeUnlock(p_entryPosition[0], p_entryPosition[1]);
    }

    /**
     * Release a write lock on a index in a table
     *
     * @param p_tableAddress address of the level 0 table
     * @param p_index row in the table
     * @return False if there was no lock or the chunk is no longer active. True on success.
     */
    private boolean writeUnlock(final long p_tableAddress, final long p_index){
        long m_offset = p_index * ENTRY_SIZE;
        long value;

        // delete write access flag
        while(true){
            value = m_rawMemory.readLongRaw(p_tableAddress, m_offset, LID_TABLE_SIZE);

            //Check if entry is alive
            if(value == FREE_ENTRY || value == ZOMBIE_ENTRY)
                return false;

            if((value & WRITE_ACCESS.BITMASK) == 0)
                return false;

            if(m_rawMemory.compareAndSwapLong(p_tableAddress, m_offset, value, value & ~WRITE_ACCESS.BITMASK))
                break;
        }

        return false;
    }

    /**
     * Get the address of the table directory
     *
     * @return Address of table directory
     */
    long getAddressTableDirectory() {
        return m_addressTableDirectory;
    }

    /**
     * Creates the NodeID table
     *
     * @return the address of the table
     */
    private long createNIDTable() {
        long ret;

        // #ifdef STATISTICS
        //SOP_CREATE_NID_TABLE.enter(NID_TABLE_SIZE);

        //MemoryManagerComponent.SOP_MALLOC.enter(NID_TABLE_SIZE);
        // #endif /* STATISTICS */
        ret = m_rawMemory.mallocRaw(NID_TABLE_SIZE);
        // #ifdef STATISTICS
        //MemoryManagerComponent.SOP_MALLOC.leave();
        // #endif /* STATISTICS */
        if (ret > SmallObjectHeap.INVALID_ADDRESS) {
            m_rawMemory.set(ret, NID_TABLE_SIZE, (byte) 0);
            m_totalMemoryTables += NID_TABLE_SIZE;
            m_tableCount++;
        }
        // #ifdef STATISTICS
        //SOP_CREATE_NID_TABLE.leave();
        // #endif /* STATISTICS */

        return ret;
    }

    /**
     * Creates a table
     *
     * @return the address of the table
     */
    private long createLIDTable() {
        long ret;

        // #ifdef STATISTICS
        //SOP_CREATE_LID_TABLE.enter(LID_TABLE_SIZE);

        //MemoryManagerComponent.SOP_MALLOC.enter(LID_TABLE_SIZE);
        // #endif /* STATISTICS */
        ret = m_rawMemory.mallocRaw(LID_TABLE_SIZE);
        // #ifdef STATISTICS
        //MemoryManagerComponent.SOP_MALLOC.leave();
        // #endif /* STATISTICS */
        if (ret > SmallObjectHeap.INVALID_ADDRESS) {
            m_rawMemory.set(ret, LID_TABLE_SIZE, (byte) 0);
            m_totalMemoryTables += LID_TABLE_SIZE;
            m_tableCount++;
        }
        // #ifdef STATISTICS
        //SOP_CREATE_LID_TABLE.leave();
        // #endif /* STATISTICS */

        return ret;
    }

    /**
     * Adds all ChunkID ranges to an ArrayList
     *
     * @param p_unfinishedCID
     *     the unfinished ChunkID
     * @param p_table
     *     the current table
     * @param p_level
     *     the current table level
     */
    private void getAllRanges(final ArrayListLong p_ret, final long p_unfinishedCID, final long p_table, final int p_level) {
        long entry;

        for (int i = 0; i < ENTRIES_PER_LID_LEVEL; i++) {
            entry = readEntry(p_table, i, LID_TABLE_SIZE);
            if (entry > 0) {

                if (p_level > 0) {
                    getAllRanges(p_ret, p_unfinishedCID + (i << BITS_PER_LID_LEVEL * p_level), ADDRESS.get(entry), p_level - 1);
                } else {
                    if (entry != ZOMBIE_ENTRY) {
                        long curCID = p_unfinishedCID + i;

                        if (p_ret.getSize() < 2) {
                            p_ret.add(curCID);
                            p_ret.add(curCID);
                        } else {
                            long prev = p_ret.get(p_ret.getSize() - 1);

                            if (prev + 1 == curCID) {
                                p_ret.set(p_ret.getSize() - 1, curCID);
                            } else {
                                p_ret.add(curCID);
                                p_ret.add(curCID);
                            }
                        }
                    }
                }
            }
        }
    }


    /**
     * Do a reverse search form a entry to a chunk id
     *
     * @param p_cidTableEntry The entry for which the CID is searched
     * @return The chunk id or 0 if no suitable chunk id was found
     */
    long reverseSearch(final long p_cidTableEntry){
        return new ReverseSearch(p_cidTableEntry).getCID();
    }

    /**
     * Update the state of a CIDTable entry
     *
     * @param chunkID Chunk ID.
     * @param state State to change.
     * @param newState New value to set .
     * @return True if state is set, false if the chunk don't exist.
     */
    boolean setState(final long chunkID, CIDTableEntry.EntryBit state, final boolean newState){
        long[] entryPosition = getAddressOfEntry(chunkID);

        return setState(entryPosition, state, newState);
    }

    /**
     * Update the state of a CIDTable entry
     *
     * @param entryPosition The position of the entry(long array: {address of table, index in table}
     * @param state State to change.
     * @param newState New value to set .
     * @return True if state is set, false if the chunk don't exist.
     */
    boolean setState(final long[] entryPosition, CIDTableEntry.EntryBit state, final boolean newState){
        assert entryPosition != null;

        long entry;
        while (true) {
            entry = m_rawMemory.readLongRaw(entryPosition[0], entryPosition[1] * ENTRY_SIZE, LID_TABLE_SIZE);

            if(entry == FREE_ENTRY && entry == ZOMBIE_ENTRY)
                return false;

            if(m_rawMemory.compareAndSwapLong(entryPosition[0], entryPosition[1] * ENTRY_SIZE, entry, state.set(entry, newState)))
                break;
        }

        return true;
    }

    /**
     * Stores free LocalIDs
     *
     * @author Florian Klein
     *         30.04.2014
     */
    private final class LIDStore {

        // Constants
        private static final int STORE_CAPACITY = 100000;

        // Attributes
        private final long[] m_localIDs;
        private int m_getPosition;
        private int m_putPosition;
        // available free lid elements stored in our array
        private int m_count;
        // This counts the total available lids in the array
        // as well as elements that are still allocated
        // (because they don't fit into the local array anymore)
        // but not valid -> zombies
        private long m_overallCount;

        // Constructors

        /**
         * Creates an instance of LIDStore
         */
        private LIDStore() {
            m_localIDs = new long[STORE_CAPACITY];
            m_getPosition = 0;
            m_putPosition = 0;
            m_count = 0;

            m_overallCount = 0;
        }

        // Methods

        /**
         * Gets a free LocalID
         *
         * @return a free LocalID
         */
        long get() {
            long ret = -1;

            if (m_overallCount > 0) {
                if (m_count == 0) {
                    fill();
                }

                if (m_count > 0) {
                    ret = m_localIDs[m_getPosition];

                    m_getPosition = (m_getPosition + 1) % m_localIDs.length;
                    m_count--;
                    m_overallCount--;
                }
            }

            return ret;
        }

        /**
         * Gets a free LocalID
         *
         * @return a free LocalID
         */
        long[] getConsecutiveLIDs(final int p_size) {
            long[] ret;
            int counter = 0;
            int visited = 0;
            long currentID;
            long lastID = -1;

            ret = new long[p_size];
            while (counter < p_size) {
                if (m_overallCount - visited < p_size - counter) {
                    ret = null;
                    break;
                }

                if (m_count == 0) {
                    fill();
                }

                if (m_count > 0) {
                    currentID = m_localIDs[m_getPosition];

                    m_getPosition = (m_getPosition + 1) % m_localIDs.length;
                    m_count--;
                    m_overallCount--;

                    if (currentID == lastID + 1 || lastID == -1) {
                        counter++;
                        lastID = currentID;
                    } else {
                        counter = 0;
                    }
                    visited++;
                }
            }

            return ret;
        }

        /**
         * Puts a free LocalID
         *
         * @param p_localID
         *     a LocalID
         * @return True if adding an entry to our local ID store was successful, false otherwise.
         */
        boolean put(final long p_localID) {
            boolean ret;

            if (m_count < m_localIDs.length) {
                m_localIDs[m_putPosition] = p_localID;

                m_putPosition = (m_putPosition + 1) % m_localIDs.length;
                m_count++;

                ret = true;
            } else {
                ret = false;
            }

            m_overallCount++;

            return ret;
        }

        /**
         * Fills the store
         */
        private void fill() {
            findFreeLIDs();
        }

        /**
         * Finds free LIDs in the CIDTable
         */
        private void findFreeLIDs() {
            findFreeLIDs(ADDRESS.get(readEntry(m_addressTableDirectory,
                    m_ownNodeID & NID_LEVEL_BITMASK, NID_TABLE_SIZE)),
                    LID_TABLE_LEVELS - 1, 0);
        }

        /**
         * Finds free LIDs in the CIDTable
         *
         * @param p_addressTable
         *     the table
         * @param p_level
         *     the table level
         * @param p_offset
         *     the offset of the LID
         * @return true if free LIDs were found, false otherwise
         */
        private boolean findFreeLIDs(final long p_addressTable, final int p_level, final long p_offset) {
            boolean ret = false;
            long localID;
            long entry;

            for (int i = 0; i < ENTRIES_PER_LID_LEVEL; i++) {
                // Read table entry
                entry = readEntry(p_addressTable, i, LID_TABLE_SIZE);

                if (p_level > 0) {
                    if (entry > 0) {
                        // Get free LocalID in the next table
                        if (!findFreeLIDs(ADDRESS.get(entry), p_level - 1, i << BITS_PER_LID_LEVEL * p_level)) {
                            // Mark the table as full
                            entry = FULL_FLAG.set(entry, true);
                            writeEntry(p_addressTable, i, entry, LID_TABLE_SIZE);
                        } else {
                            ret = true;
                        }
                    }
                } else {
                    // check if we got an entry referencing a zombie
                    if (entry == ZOMBIE_ENTRY) {
                        localID = p_offset + i;

                        // cleanup zombie in table
                        writeEntry(p_addressTable, i, FREE_ENTRY, LID_TABLE_SIZE);

                        m_localIDs[m_putPosition] = localID;
                        m_putPosition = (m_putPosition + 1) % m_localIDs.length;
                        m_count++;

                        ret = true;
                    }
                }

                if (m_count == m_localIDs.length || m_count == m_overallCount) {
                    break;
                }
            }

            return ret;
        }
    }

    /**
     * Cache for translated addresses
     */
    private static final class TranslationCache {

        private long[] m_chunkIDs;
        private long[] m_tableLevel0Addr;
        private int m_cachePos;

        /**
         * Constructor
         *
         * @param p_size
         *     Number of entries for the cache
         */
        TranslationCache(final int p_size) {
            m_chunkIDs = new long[p_size];
            m_tableLevel0Addr = new long[p_size];
            m_cachePos = 0;

            for (int i = 0; i < p_size; i++) {
                m_chunkIDs[i] = -1;
                m_tableLevel0Addr[i] = -1;
            }
        }

        /**
         * Try to get the table level 0 entry for the chunk id
         *
         * @param p_chunkID
         *     Chunk id for cache lookup of table level 0
         * @return Address of level 0 table or -1 if not cached
         */
        long getTableLevel0(final long p_chunkID) {
            long tableLevel0IDRange = p_chunkID >> BITS_PER_LID_LEVEL;

            for (int i = 0; i < m_chunkIDs.length; i++) {
                if (m_chunkIDs[i] == tableLevel0IDRange) {
                    return m_tableLevel0Addr[i];
                }
            }

            return -1;
        }

        /**
         * Put a new entry into the cache
         *
         * @param p_chunkID
         *     Chunk id of the table level 0 to be cached
         * @param p_addressTable
         *     Address of the level 0 table
         */
        void putTableLevel0(final long p_chunkID, final long p_addressTable) {
            m_chunkIDs[m_cachePos] = p_chunkID >> BITS_PER_LID_LEVEL;
            m_tableLevel0Addr[m_cachePos] = p_addressTable;
            m_cachePos = (m_cachePos + 1) % m_chunkIDs.length;
        }

        /**
         * Invalidate a cache entry
         *
         * @param p_chunkID
         *     Chunk id of the table level 0 to invalidate
         */
        void invalidateEntry(final long p_chunkID) {
            long tableLevel0IDRange = p_chunkID >> BITS_PER_LID_LEVEL;

            for (int i = 0; i < m_chunkIDs.length; i++) {
                if (m_chunkIDs[i] == tableLevel0IDRange) {
                    m_tableLevel0Addr[i] = -1;
                    m_chunkIDs[i] = -1;
                    break;
                }
            }
        }
    }

    /**
     * Search a chunk ID for a address, with DFS
     */
    private final class ReverseSearch {
        final long m_address;
        long chunkID = -1;
        private boolean found;

        /**
         * Constructor
         *
         * @param p_entry Entry for which the CID is searched.
         */
        private ReverseSearch(final long p_entry){
            m_address = ADDRESS.get(p_entry);
            found = false;
        }

        /**
         * Search a chunk ID with DFS
         *
         * @return the matching chunk ID or -1 if no suitable chunk id was found
         */
        private long getCID(){
            for (int i = 0; i < ENTRIES_FOR_NID_LEVEL && !found; i++) {
                long entry = readEntry(m_addressTableDirectory, i, NID_TABLE_SIZE);
                if(entry != FREE_ENTRY && entry != ZOMBIE_ENTRY)
                    getCID(LID_TABLE_LEVELS-1, ADDRESS.get(entry), i);
            }

            return chunkID;
        }

        /**
         * Recursive depth first search for a chunk ID
         *
         * @param level LID level.
         * @param tableAddress Address of the table.
         * @param chunkID Current known chunk ID part.
         */
        private void getCID(final int level, final long tableAddress, final long chunkID){
            long entry;

            for (int i = 0; i < ENTRIES_PER_LID_LEVEL && !found; i++) {
                entry = readEntry(tableAddress, i, LID_TABLE_SIZE);
                if(entry != FREE_ENTRY && entry != ZOMBIE_ENTRY) {
                    if (level > 0) {
                        long address = ADDRESS.get(entry);
                        getCID(level - 1, address, (chunkID << BITS_PER_LID_LEVEL) | i);
                    } else if (ADDRESS.get(entry) == m_address) {
                        found = true;
                        this.chunkID = (chunkID << BITS_PER_LID_LEVEL) | i;
                    }
                }
            }
        }
    }
}
