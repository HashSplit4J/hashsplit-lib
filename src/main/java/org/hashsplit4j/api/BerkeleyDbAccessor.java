/*
 * Copyright (C) McEvoy Software Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.hashsplit4j.api;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;

/**
 * The data accessor class for the entity model
 *
 * @author <a href="mailto:sondn@exoplatform.com">Ngoc Son Dang</a>
 * @version BerkeleyDbAccessor.java Dec 12, 2013
 */
public class BerkeleyDbAccessor {

    private static final String SUBGROUP_SECONDARY_INDEX = "subGroup";
    private static final String STATUS_SECONDARY_INDEX = "status";
    private static final String PARENT_SECONDARY_INDEX = "parent";

    private final PrimaryIndex<String, Blob> blobByIndex;

    private final PrimaryIndex<String, HashGroup> groupByIndex;

    private final PrimaryIndex<String, SubGroup> subGroupByIndex;

    private final SecondaryIndex<String, String, Blob> blobBySubGroup;  // Secondary key for Blob classes
    // Last field in the getSecondaryIndex() method must be
    // the name of a class member; in this case, an Blob.class
    // data member.

    private final SecondaryIndex<String, String, HashGroup> groupByStatus;  // Secondary key for HashGroup classes

    private final SecondaryIndex<String, String, SubGroup> subGroupByParent; // Secondary key for SubGroup classes

    /**
     * Open the indices
     *
     * @param store
     * @throws DatabaseException
     */
    public BerkeleyDbAccessor(EntityStore store) throws DatabaseException {
        this.blobByIndex = store.getPrimaryIndex(String.class, Blob.class);
        this.blobBySubGroup = store.getSecondaryIndex(blobByIndex, String.class, SUBGROUP_SECONDARY_INDEX);
        this.groupByIndex = store.getPrimaryIndex(String.class, HashGroup.class);
        this.groupByStatus = store.getSecondaryIndex(groupByIndex, String.class, STATUS_SECONDARY_INDEX);
        this.subGroupByIndex = store.getPrimaryIndex(String.class, SubGroup.class);
        this.subGroupByParent = store.getSecondaryIndex(subGroupByIndex, String.class, PARENT_SECONDARY_INDEX);
    }

    public void addToBlobByIndex(Blob blobEntity) {
        PrimaryIndex<String, Blob> bI = getBlobByIndex();
        bI.putNoOverwrite(blobEntity);
    }

    public Blob getFromBlobByIndex(String hash) {
        return blobByIndex.get(hash);
    }

    public PrimaryIndex<String, Blob> getBlobByIndex() {
        return blobByIndex;
    }

    public SecondaryIndex<String, String, Blob> getBlobBySubGroup() {
        return blobBySubGroup;
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
