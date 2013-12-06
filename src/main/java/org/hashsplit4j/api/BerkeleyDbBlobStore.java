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

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import java.util.List;

public class BerkeleyDbBlobStore implements BlobStore {

    private static final Charset CHARSET_UTF = Charset.forName("UTF-8");

    private final Environment env;

    private final Database db;

    /**
     * Default folder for stored files
     */
    private final File dbDir;

    private final long cacheSize;

    public BerkeleyDbBlobStore(File dbDir, long cacheSize) {
        this.dbDir = dbDir;
        this.cacheSize = cacheSize;
        this.env = createDBEnvironment();
        this.db = openDatabase();
    }

    @Override
    public void setBlob(String hash, byte[] bytes) {
        if (hash == null || bytes == null) {
            throw new RuntimeException("Key and value can not be null for setBlob() function");
        }

        DatabaseEntry key = new DatabaseEntry(hash.getBytes(CHARSET_UTF));
        DatabaseEntry data = new DatabaseEntry(bytes);
        db.putNoOverwrite(null, key, data);
    }

    @Override
    public byte[] getBlob(String hash) {
        if (hash == null) {
            throw new RuntimeException("Key can not be null for setBlob() function");
        }

        DatabaseEntry search = new DatabaseEntry();
        db.get(null, new DatabaseEntry(hash.getBytes(CHARSET_UTF)), search, LockMode.DEFAULT);
        return search.getData();
    }

    @Override
    public boolean hasBlob(String hash) {
        byte[] bytes = getBlob(hash);
        if (bytes != null && bytes.length >= 0) {
            return true;
        }

        return false;
    }

    public void close() {
        if (db != null) {
            db.close();
        }

        if (env != null) {
            env.cleanLog();
            env.close();
        }
    }

    /**
     * Inits the databased environment used for all databases.
     *
     * @return environment
     */
    private Environment createDBEnvironment() {
        if (!dbDir.exists()) {
            if (!dbDir.mkdirs()) {
                throw new RuntimeException("The directory " + dbDir + " does not exist.");
            }
        }

        EnvironmentConfig envCfg = new EnvironmentConfig();
        envCfg.setAllowCreate(true);
        envCfg.setSharedCache(true);
        envCfg.setCacheSize(cacheSize);
        envCfg.setDurability(Durability.COMMIT_SYNC);
        return new Environment(dbDir, envCfg);
    }

    private Database openDatabase() {
        DatabaseConfig dbCfg = new DatabaseConfig();
        dbCfg.setAllowCreate(true);
        dbCfg.setSortedDuplicates(false);
        dbCfg.setOverrideDuplicateComparator(false);
        return env.openDatabase(null, "", dbCfg);
    }
    
    /**
     * Create any missing hashes for blobs and groups. Note that it is assumed
     * that any group insertions will have deleted group items 
     * 
     * @return 
     */
    public void generateHashes() {
        
    }
    
    /**
     * Get the group hashes for the initial hash prefix (ie first 3 chars). Return
     * only those currently persisted ie do not dynamically generate any missing hashes
     * 
     * @return 
     */
    public List<HashGroup> getRootGroups() {
        return null;
    }
    
    /**
     * Get the hash groups for the given root group
     * 
     * @param parent
     * @return 
     */
    public List<HashGroup> getSubGroups(String parent) {
        return null;
    }
    
    /**
     * Get the blob hashes for the sub group name
     * 
     * @param subGroupName
     * @return 
     */
    public List<String> getBlobHashes(String subGroupName) {
        return null;
    }
    
    /**
     * Represents a hash prefix (ie the first n digits) common to a list
     * of hashes, and the hash of the text formed by those hashes.
     * 
     * For example, assume the following hashes were inserted into the blobstore:
     *  0123456
     *  012345c
     *  0125432
     *  cce2345
     *  cceeeee
     *  
     *  Then, assuming n=3, there will be 2 root groups - 012 and cce.
     * 
     *  The 012 group would contain 2 groups - 012345 and 012543
     * 
     *  This forms a hierarchy as follows:
     *  root (the blobstore itself)
     *    - first level groups
     *      - second level groups
     *        - actual blobs
     * 
     *  The BlobStore itself and each group has a hash. The hash is formed by concentating
     *  its children in the hierarchy above with their hashes in this form:
     *  
     *  {name},{hash}
     *  
     *  Where the name is the name of the group, and the hash is the hash of this group
     * 
     *  Note that if a hash exists it is assumed to be accurate. This means that
     *  hashes must either be deleted or recalculated when new blobs are inserted
     *  It will often be inefficient to recalculate hashes on every insertion, and 
     *  would be unnecessary because syncs are only occasional, so instead we assume
     *  they will only be recaculated on demand.
     */
    public class HashGroup {
        private final String name;
        private final String contentHash;

        public HashGroup(String name, String contentHash) {
            this.name = name;
            this.contentHash = contentHash;
        }

        public String getContentHash() {
            return contentHash;
        }

        public String getName() {
            return name;
        }        
    }
}
