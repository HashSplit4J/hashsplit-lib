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

public class BlobAccessor {
  
    private static final String GROUP_SECONDARY_INDEX = "group";
    private static final String SUBGROUP_SECONDARY_INDEX = "subGroup";
    
    // Blob Accessors
    PrimaryIndex<String, Blob> primaryByIndex;
    SecondaryIndex<String, String, Blob> primaryByGroup;
    SecondaryIndex<String, String, Blob> primaryBySubGroup;
    
    // Hash Group Accessors
    PrimaryIndex<String, HashGroup> hashGroupByIndex;

    /**
     * Open the indices
     * 
     * @param store
     * @throws DatabaseException
     */
    public BlobAccessor(EntityStore store) throws DatabaseException {
        primaryByIndex = store.getPrimaryIndex(String.class, Blob.class);
        // Secondary key for Blob classes
        // Last field in the getSecondaryIndex() method must be
        // the name of a class member; in this case, an Blob.class
        // data member.
        primaryByGroup = store.getSecondaryIndex(primaryByIndex, String.class, 
            GROUP_SECONDARY_INDEX);
        primaryBySubGroup = store.getSecondaryIndex(primaryByIndex, String.class, 
            SUBGROUP_SECONDARY_INDEX);
        
        hashGroupByIndex = store.getPrimaryIndex(String.class, HashGroup.class);
    }
}
