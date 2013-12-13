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

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.sleepycat.persist.EntityCursor;

public class BerkeleyDbBlobStore implements BlobStore {
	
    private final int nPrefGroup;
    private final int nPrefSubGroup;

    private BerkeleyDbAccessor dbAccessor;

    /**
     * Encapsulates the environment and data store
     */
    private BerkeleyDbEnv dbEnv = new BerkeleyDbEnv();

    public BerkeleyDbBlobStore(File envHome, int nPrefGroup, int nPrefSubGroup) {
        this.nPrefGroup = nPrefGroup;
        this.nPrefSubGroup = nPrefSubGroup;

        dbEnv.openEnv(envHome,  // path to the environment home
                false);         // Environment read-only?

        // Open the data accessor. This is used to retrieve
        // persistent objects.
        dbAccessor = new BerkeleyDbAccessor(dbEnv.getEntityStore());
    }

    @Override
    public void setBlob(String hash, byte[] bytes) {
        if (hash == null || bytes == null)
        	throw new RuntimeException("Key and value can not be null for store blob function");

        // Get root group's name from hash
        String group = hash.substring(0, nPrefGroup);
        // Get sub group's name from hash
        String subGroup = hash.substring(0, nPrefSubGroup);
        
        // Put it in the store. Note that this causes our secondary key
        // to be automatically updated for us.
        dbAccessor.getBlobByIndex().putNoOverwrite(new Blob(hash, group, subGroup, bytes));
        
        // When insert a blob, insert the blob in one table and also insert into a key table.
        // Where the key table has the group and the blob's hash.
        // If doesn't exist, we just insert to DB with 'INVALID' status and should be removed if existing in the BerkeleyDB
        removeMissingHashes(group, subGroup);
        
        // So we should ensure that anything affected is removed
        dbAccessor.getGroupByIndex().putNoOverwrite(new HashGroup(group, null, Status.INVALID));
        dbAccessor.getSubGroupByIndex().putNoOverwrite(new SubGroup(subGroup, group, null, Status.INVALID));
    }

    @Override
    public byte[] getBlob(String hash) {
        if (hash == null)
        	throw new RuntimeException("Key can not be null for get blob function");

        // Use the Blob's hash primary key to retrieve these objects
        Blob blob = dbAccessor.getBlobByIndex().get(hash);
        if (blob == null)
            return null;
        
        return blob.getBytes();
    }

    @Override
    public boolean hasBlob(String hash) {
        byte[] bytes = getBlob(hash);
        if (bytes != null && bytes.length > 0)
        	return true;

        return false;
    }

    /**
     * Close the database environment and database store transaction
     */
    public void closeEnv() {
        dbEnv.closeEnv();
    }
    
    /**
     * Remove all of files for the given file directory
     * 
     * @param envHome
     */
    public void removeDbFiles(File envHome) {
        dbEnv.removeDbFiles(envHome);
    }

    /**
     * Create any missing hashes for blobs and groups. Note that it is assumed
     * that any group insertions will have deleted group items
     * 
     * @return
     */
    public void generateHashes() {
    	// Recalculate or regenerate hashes for the groups is missing hash
    	// Get a list of root groups with 'INVALID' status then recalculate or regenerate hashes for 'INVALID' root groups
    	EntityCursor<HashGroup> entities = dbAccessor.getGroupByStatus().subIndex(Status.INVALID).entities();
    	try {
			for (HashGroup hashGroup : entities) {
				// Get a list of sub groups for the given root group's name
				List<HashGroup> subGroups = getSubGroups(hashGroup.getName());
				
				// Recalculate or regenerate hashes for each root group bases on a list of sub groups,
				// then store it into berkeleydb.
				String recalHash = Crypt.toHexFromBlob(subGroups);
				
				// Remove old root group in the 'RootGroup' table from berkeleydb then put updated group into that table
				dbAccessor.getGroupByIndex().delete(hashGroup.getName());
				dbAccessor.getGroupByIndex().putNoOverwrite(new HashGroup(hashGroup.getName(), recalHash, Status.VALID));
			}
		} finally {
			entities.close();
		}
    }

    /**
     * Get the group hashes for the initial hash prefix (ie first 3 chars).
     * Return only those currently persisted ie do not dynamically generate any
     * missing hashes
     * 
     * E.g	+-----------+---------------+---------------+
     * 		|	NAME	|	CONTENT		|	STATUS		|
     * 		+-----------+---------------+---------------+
     * 		|	012a	|	xxxxxx		|	VALID		|
     *		|	012b	|	xxxxxx		|	VALID		|
     *		|	012c	|	xxxxxx		|	VALID		|
     *		|	012d	|	xxxxxx		|	VALID		|
     *		+-----------+---------------+---------------+
     *
     * We have a group 012 and have four sub group 012axx, 012bxx, 012cxx, 012dxx
     * 
     * @return
     */
    public List<HashGroup> getRootGroups() {
        List<HashGroup> groups = new ArrayList<HashGroup>();
        // Get a cursor that will walk every root group object in the store.
        // Return only those currently generated hashes.
        EntityCursor<HashGroup> entities = dbAccessor.getGroupByStatus().subIndex(Status.VALID).entities();
        try {
            Iterator<HashGroup> iterator = entities.iterator();
            if (iterator instanceof List)
            	return (List<HashGroup>) iterator;

            if (iterator != null) {
                while (iterator.hasNext()) {
                    groups.add(iterator.next());
                }
            }
        } finally {
            entities.close();
        }
        return groups;
    }

    /**
     * Get the hash groups for the given root group's name
     * 
     * @param parent
     * @return
     */
    public List<HashGroup> getSubGroups(String parent) {
        List<HashGroup> groups = new ArrayList<HashGroup>();
        if (parent != null && parent.length() > 0) {
        	// Return a list of sub groups for the given parent group's name
        	EntityCursor<SubGroup> entities = dbAccessor.getSubGroupByParent().subIndex(parent).entities();
        	try {
				for (SubGroup subGroup : entities) {
					// Should be recalculate or regenerate hash for sub group 
					// if sub group is not missing hash.
					if (subGroup.getStatus().equals(Status.INVALID)) {
						getBlobHashes(subGroup.getName());
						// Reset properties of sub group
						subGroup = dbAccessor.getSubGroupByIndex().get(subGroup.getName());
					}
					
					groups.add(new HashGroup(subGroup.getName(), subGroup.getContentHash(), 
							subGroup.getStatus()));
				}
			} finally {
				entities.close();
			}
        }
        return groups;
    }

    /**
     * Get the blob hashes for the sub group's name
     * 
     * @param subGroupName
     * @return
     */
    public List<String> getBlobHashes(String subGroupName) {
        List<String> hashes = new ArrayList<String>();
        if (subGroupName != null && !subGroupName.isEmpty()) {
            // Use the sub group name secondary key to retrieve these objects of Blob.class
            EntityCursor<Blob> entities = dbAccessor.getBlobBySubGroup().subIndex(subGroupName).entities();
            try {
                for (Blob blob : entities) {
                	hashes.add(blob.getHash());
                }
                
                // Removed sub group if it's existing
                dbAccessor.getSubGroupByIndex().delete(subGroupName);
                
                // A second level group is just represented by a list of hashes of the blobs inside it
                dbAccessor.getSubGroupByIndex().putNoOverwrite(new SubGroup(subGroupName, 
                		subGroupName.substring(0, nPrefGroup), Crypt.toHexFromHash(hashes), Status.VALID));
            } finally {
                entities.close();
            }
        }
        return hashes;
    }
    
    /**
     * This should delete that root group and sub group because the original root group
     * and sub group are no longer valid
     * 
     * @param hash
     */
    private void removeMissingHashes(String group, String subGroup) {
        if (group != null)
        	dbAccessor.getGroupByIndex().delete(group);
        
        if (subGroup != null)
        	dbAccessor.getSubGroupByIndex().delete(subGroup);
    }
}
