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
import java.io.FileNotFoundException;
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
        	throw new RuntimeException("Key and data can not be null for store blob function");

        String group = hash.substring(0, nPrefGroup);
        String subGroup = hash.substring(0, nPrefSubGroup);
        
        // Put it in the store. Note that this causes our secondary key
        // to be automatically updated for us.
        dbAccessor.getBlobByIndex().putNoOverwrite(new Blob(hash, group, subGroup, bytes));
        
        // When insert a blob, insert the blob in one table and also insert into a key table.
        // Where the key table has the group and the blob's hash. If doesn't exist, 
        // we just insert to DB with 'INVALID' status and should delete that root group 
        // and sub group because the original root group and sub group are no longer valid
        dbAccessor.getGroupByIndex().put(new HashGroup(group, null, Status.INVALID));
        dbAccessor.getSubGroupByIndex().put(new SubGroup(subGroup, group, null, Status.INVALID));
    }

    @Override
    public byte[] getBlob(String hash) {
        if (hash == null)
        	throw new RuntimeException("Key can not be null for get blob function");

        Blob blob = dbAccessor.getBlobByIndex().get(hash);
        if (blob == null)
            return null;
        
        return blob.getBytes();
    }

    @Override
    public boolean hasBlob(String hash) {
        return dbAccessor.getBlobByIndex().contains(hash);
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
     * E.g   +-----------+---------------+---------------+
     *       |   NAME    |   CONTENT     |   STATUS      |
     *       +-----------+---------------+---------------+
     *       |   012     |   xxxxxx      |   INVALID     |
     *       |   ccc     |   xxxxxx      |   INVALID     |
     *       |   xyz     |   xxxxxx      |   INVALID     |
     *       |   abc     |   xxxxxx      |   INVALID     |
     *       +-----------+---------------+---------------+
     *      
     * @return
     */
    public void generateHashes() {
    	EntityCursor<HashGroup> entities = dbAccessor.getGroupByStatus().subIndex(Status.INVALID).entities();
    	try {
			for (HashGroup hashGroup : entities) {
				List<HashGroup> subGroups = getSubGroups(hashGroup.getName());
				
				String recalHash = Crypt.toHexFromBlob(subGroups);
				hashGroup.setContentHash(recalHash);
				hashGroup.setStatus(Status.VALID);
				dbAccessor.getGroupByIndex().put(hashGroup);
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
     * 		|	012 	|	xxxxxx		|	VALID		|
     *		|	ccc 	|	xxxxxx		|	VALID		|
     *		|	xyz 	|	xxxxxx		|	VALID		|
     *		|	abc 	|	xxxxxx		|	VALID		|
     *		+-----------+---------------+---------------+
     * 
     * @return
     */
    public List<HashGroup> getRootGroups() {
        List<HashGroup> groups = new ArrayList<HashGroup>();
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
        EntityCursor<SubGroup> entities = dbAccessor.getSubGroupByParent().subIndex(parent).entities();
        try {
            for (SubGroup subGroup : entities) {
                if (subGroup.getStatus().equals(Status.INVALID)) {
                    getBlobHashes(subGroup.getName());
                    subGroup = dbAccessor.getSubGroupByIndex().get(subGroup.getName());
                }
                
                HashGroup hashGroup = new HashGroup(subGroup.getName(), 
                        subGroup.getContentHash(), subGroup.getStatus());
                groups.add(hashGroup);
            }
        } finally {
            entities.close();
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
        EntityCursor<Blob> entities = dbAccessor.getBlobBySubGroup().subIndex(subGroupName).entities();
        try {
            for (Blob blob : entities) {
                hashes.add(blob.getHash());
            }
            
            String recalHash = Crypt.toHexFromHash(hashes);
            String rootGroup = subGroupName.substring(0, nPrefGroup);
            SubGroup subGroup = new SubGroup(subGroupName, rootGroup, recalHash, Status.VALID);
            dbAccessor.getSubGroupByIndex().put(subGroup);
        } finally {
            entities.close();
        }
        return hashes;
    }

    /**
     * Scan the given directory for sub folders (recursively) and files, and import
     * any files into this blob store
     * 
     * DO NOT IMPORT:
     *  - hidden files
     *  - files that start with a dot
     * 
     * @param dir
     * @return 
     */
    public int importFiles(File dir) {
    	if (!dir.exists())
    		throw new RuntimeException("No such file or directory " + dir);
    	
    	int totalImports = 0;
    	if (!dir.isDirectory()) {
    		if (!dir.isHidden()) {
    			String hash = dir.getName();
    			if (hash.contains("."))
    				throw new RuntimeException("The name should be calcaulated is SHA1 of its contents. "
    						+ "It could not contains '.' character");
    			
    			byte[] contents = FileUtils.read(dir);
    			
    			// Put its contents into BerkeleyDB
    			setBlob(hash, contents);
    			// Only one Blob has been imported to BerkeleyDB
    			totalImports += 1;
    		}
    	}
    	
    	File[] files = dir.listFiles();
    	for (File file : files) {
    		if (file.isDirectory())
    			importFiles(dir);
    	}
    	
    	return totalImports;
    }
}
