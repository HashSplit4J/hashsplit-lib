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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.EntityCursor;

public class BerkeleyDbBlobStore implements BlobStore {

    private static final Charset CHARSET_UTF = Charset.forName("UTF-8");

    private final int nPrefGroup;
    private final int nPrefSubGroup;

    private BerkeleyDbAccessor dbAccessor;

    // Encapsulates the environment and data store.
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
        if (hash == null || bytes == null) {
            throw new RuntimeException("Key and value can not be null for store blob function");
        }

        String group = hash.substring(0, nPrefGroup);
        // Put it in the store. Note that this causes our secondary key
        // to be automatically updated for us.
        dbAccessor.getBlobByIndex().putNoOverwrite(new Blob(hash, group, 
                hash.substring(0, nPrefSubGroup), new String(bytes, CHARSET_UTF)));
        
        // So we should ensure that anything affected is removed
        removeMissingHash(group);       
    }

    @Override
    public byte[] getBlob(String hash) {
        if (hash == null) {
            throw new RuntimeException("Key can not be null for get blob function");
        }

        // Use the Blob's hash primary key to retrieve these objects.
        Blob blob = dbAccessor.getBlobByIndex().get(hash);
        if (blob == null)
            return null;

        byte[] bytes = blob.getContents().getBytes(CHARSET_UTF);
        return bytes;
    }

    @Override
    public boolean hasBlob(String hash) {
        byte[] bytes = getBlob(hash);
        if (bytes != null && bytes.length >= 0) {
            return true;
        }

        return false;
    }

    public void closeEnv() {
        dbEnv.closeEnv();
    }
    
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
        Map<String, List<Blob>> entities = getBlobByGroup();
        if (entities != null && !entities.isEmpty()) {
            String name = null;
            List<Blob> childrens = null;
            Iterator<Entry<String, List<Blob>>> iterator = entities.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry entry = (Map.Entry) iterator.next();

                // The name of the group, and the hash is the hash of this group
                name = (String) entry.getKey();
                childrens = (List<Blob>) entry.getValue();
                // Storage into Hash Group table without overwrite
                setGroupNoOverwrite(name, childrens);
            }
        }
    }

    /**
     * Get the group hashes for the initial hash prefix (ie first 3 chars).
     * Return only those currently persisted ie do not dynamically generate any
     * missing hashes
     * 
     * @return
     */
    public List<HashGroup> getRootGroups() {
        List<HashGroup> groups = new ArrayList<HashGroup>();
        // Get a cursor that will walk every hash group object in the store
        EntityCursor<HashGroup> entities = dbAccessor.getGroupByIndex().entities();
        try {
            Iterator<HashGroup> iterator = entities.iterator();
            if (iterator instanceof List) {
                return (List<HashGroup>) iterator;
            }

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
     * Get the hash groups for the given root group
     * 
     * @param parent
     * @return
     */
    public List<HashGroup> getSubGroups(String parent) {
        List<HashGroup> groups = new ArrayList<HashGroup>();
        if (parent != null && parent.length() > 0) {
            Map<String, List<Blob>> entities = getBlobBySubGroup(parent);
            Iterator<Entry<String, List<Blob>>> iterator = entities.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry entry = (Map.Entry) iterator.next();
                String name = (String) entry.getKey();
                List<Blob> childrens = (List<Blob>) entry.getValue();
                groups.add(new HashGroup(name, Crypt.toHexFromArray(childrens)));
            }
        }
        return groups;
    }

    /**
     * Get the blob hashes for the sub group name
     * 
     * @param subGroupName
     * @return
     */
    public List<String> getBlobHashes(String subGroupName) {
        List<String> hashes = new ArrayList<String>();
        if (subGroupName != null && !subGroupName.isEmpty()) {
            // Use the sub group name secondary key to retrieve these objects
            EntityCursor<Blob> entities = dbAccessor.getBlobBySubGroup().subIndex(subGroupName).entities();
            try {
                for (Blob blob : entities) {
                    hashes.add(blob.getHash());
                }
            } finally {
                entities.close();
            }
        }
        return hashes;
    }
    
    /**
     * Get all the blobs are sorted by group's nane in the store
     * 
     * @return
     * @throws DatabaseException
     */
    private Map<String, List<Blob>> getBlobByGroup() throws DatabaseException {
        // Get a cursor that will walk every blob object in the store
        EntityCursor<Blob> entities = dbAccessor.getBlobByIndex().entities();
        Map<String, List<Blob>> map = new HashMap<String, List<Blob>>();
        try {
            for (Blob blob : entities) {
                String group = blob.getGroup();
                if (map.get(group) == null) {
                    map.put(group, new ArrayList<Blob>());
                }

                map.get(group).add(blob);
            }
        } finally {
            entities.close();
        }
        return map;
    }
    
    /**
     * Get the blobs are sorted by sub group's name for the given root group's name
     * 
     * @param parent
     *            - the root group's name
     * @return
     * @throws DatabaseException
     */
    private Map<String, List<Blob>> getBlobBySubGroup(String parent) throws DatabaseException {
        // Use the group name secondary key to retrieve these objects
        EntityCursor<Blob> entities = dbAccessor.getBlobByGroup().subIndex(parent).entities();
        Map<String, List<Blob>> map = new HashMap<String, List<Blob>>();
        try {
            for (Blob blob : entities) {
                String subGroup = blob.getSubGroup();
                if (map.get(subGroup) == null) {
                    map.put(subGroup, new ArrayList<Blob>());
                }

                map.get(subGroup).add(blob);
            }
        } finally {
            entities.close();
        }
        return map;
    }
    
    /**
     * Put it in the store. Because we do not explicitly set
     * a transaction here, and because the store was opened
     * with transactional support, auto commit is used for each
     * write to the store.
     * 
     * @param name
     * @param childrens
     */
    private void setGroupNoOverwrite(String name, List<Blob> childrens) {
        HashGroup hashGroup = new HashGroup(name, Crypt.toHexFromArray(childrens));
        if (hashGroup != null) {
            dbAccessor.getGroupByIndex().putNoOverwrite(hashGroup);
        }
    }
    
    /**
     * This should delete that root group because the original root group is
     * no longer valid
     * 
     * @param hash
     */
    private void removeMissingHash(String group) {
        if (dbAccessor.getGroupByIndex().get(group) != null) {
            dbAccessor.getGroupByIndex().delete(group);
        }
    }
}
