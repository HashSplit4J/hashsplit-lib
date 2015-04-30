/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hashsplit4j.api;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;

/**
 *
 * @author dylan
 */
public class BerkeleyHashDbAccessor {

    private static final String SUBGROUP_SECONDARY_INDEX = "subGroup";
    private static final String STATUS_SECONDARY_INDEX = "status";
    private static final String PARENT_SECONDARY_INDEX = "parent";

    private final PrimaryIndex<String, Hash> hashByIndex;
    private final PrimaryIndex<String, HashGroup> groupByIndex;
    private final PrimaryIndex<String, SubGroup> subGroupByIndex;

    private final SecondaryIndex<String, String, Hash> hashBySubGroup;
    private final SecondaryIndex<String, String, HashGroup> groupByStatus;
    private final SecondaryIndex<String, String, SubGroup> subGroupByParent;

    public BerkeleyHashDbAccessor(EntityStore store) throws DatabaseException {
        this.hashByIndex = store.getPrimaryIndex(String.class, Hash.class);
        this.hashBySubGroup = store.getSecondaryIndex(hashByIndex, String.class, SUBGROUP_SECONDARY_INDEX);
        this.groupByIndex = store.getPrimaryIndex(String.class, HashGroup.class);
        this.groupByStatus = store.getSecondaryIndex(groupByIndex, String.class, STATUS_SECONDARY_INDEX);
        this.subGroupByIndex = store.getPrimaryIndex(String.class, SubGroup.class);
        this.subGroupByParent = store.getSecondaryIndex(subGroupByIndex, String.class, PARENT_SECONDARY_INDEX);
    }

    public PrimaryIndex<String, Hash> getHashByIndex() {
        return hashByIndex;
    }

    public void addToHashByIndex(Hash hashEntity) {
        hashByIndex.putNoOverwrite(hashEntity);
    }

    public Hash getFromHashByIndex(String hash) {
        return hashByIndex.get(hash);
    }

    public boolean containsHashByIndex(String hash) {
        return hashByIndex.contains(hash);
    }

    public SecondaryIndex<String, String, Hash> getHashBySubGroup() {
        return hashBySubGroup;
    }

    public PrimaryIndex<String, HashGroup> getGroupByIndex() {
        return groupByIndex;
    }

    public SecondaryIndex<String, String, HashGroup> getGroupByStatus() {
        return groupByStatus;
    }

    public PrimaryIndex<String, SubGroup> getSubGroupByIndex() {
        return subGroupByIndex;
    }

    public SecondaryIndex<String, String, SubGroup> getSubGroupByParent() {
        return subGroupByParent;
    }

}
