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
  
    private static final String GROUP_SECONDARY_INDEX = "group";
    private static final String SUBGROUP_SECONDARY_INDEX = "subGroup";

    // Blob Accessors
    private PrimaryIndex<String, Blob> blobByIndex;
    private SecondaryIndex<String, String, Blob> blobByGroup;
    private SecondaryIndex<String, String, Blob> blobBySubGroup;

    // Hash Group Accessors
    private PrimaryIndex<String, HashGroup> groupByIndex;

    /**
     * Open the indices
     * 
     * @param store
     * @throws DatabaseException
     */
    public BerkeleyDbAccessor(EntityStore store) throws DatabaseException {
        blobByIndex = store.getPrimaryIndex(String.class, Blob.class);
        // Secondary key for Blob classes
        // Last field in the getSecondaryIndex() method must be
        // the name of a class member; in this case, an Blob.class
        // data member.
        blobByGroup = store.getSecondaryIndex(blobByIndex, String.class,
                GROUP_SECONDARY_INDEX);
        blobBySubGroup = store.getSecondaryIndex(blobByIndex, String.class,
                SUBGROUP_SECONDARY_INDEX);

        groupByIndex = store.getPrimaryIndex(String.class, HashGroup.class);
    }

    public PrimaryIndex<String, Blob> getBlobByIndex() {
        return blobByIndex;
    }

    public SecondaryIndex<String, String, Blob> getBlobByGroup() {
        return blobByGroup;
    }

    public SecondaryIndex<String, String, Blob> getBlobBySubGroup() {
        return blobBySubGroup;
    }

    public PrimaryIndex<String, HashGroup> getGroupByIndex() {
        return groupByIndex;
    }
}
