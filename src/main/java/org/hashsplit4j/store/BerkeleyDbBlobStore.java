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
package org.hashsplit4j.store;

import org.hashsplit4j.store.berkeleyDbEnv.BerkeleyDbEnv;
import org.hashsplit4j.store.berkeleyDbEnv.BerkeleyDbAccessor;
import org.hashsplit4j.store.berkeleyDbEnv.Blob;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.persist.EntityCursor;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.hashsplit4j.api.*;

public class BerkeleyDbBlobStore implements BlobStore, ReceivingBlobStore {

    private final Logger log = LoggerFactory.getLogger(BerkeleyDbBlobStore.class);

    private final int nPrefGroup;
    private final int nPrefSubGroup;

    private final BerkeleyDbAccessor dbAccessor;

    private final ScheduledExecutorService scheduler = Executors
            .newScheduledThreadPool(1);

    private Date lastCommit = new Date();
    private Boolean doCommit = false;
    private int commitCount = 0;

    private final ScheduledFuture<?> taskHandle;

    /**
     * Encapsulates the environment and data store
     */
    private final BerkeleyDbEnv dbEnv = new BerkeleyDbEnv();

    public BerkeleyDbBlobStore(File envHome, int nPrefGroup, int nPrefSubGroup) {
        this.taskHandle = scheduler.scheduleAtFixedRate(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            writeToDisk();
                        } catch (Exception ex) {
                            log.warn("Error writing db changes to disk", ex);
                        }
                    }
                }, 0, 15, TimeUnit.SECONDS);

        this.nPrefGroup = nPrefGroup;
        this.nPrefSubGroup = nPrefSubGroup;

        dbEnv.openEnv(envHome, // path to the environment home
                false);         // Environment read-only?

        // Open the data accessor. This is used to retrieve
        // persistent objects.
        dbAccessor = new BerkeleyDbAccessor(dbEnv.getEntityStore());
    }

    @Override
    public void setBlob(String hash, byte[] bytes) {
        if (hash == null || bytes == null) {
            throw new RuntimeException("Key and data can not be null for store blob function");
        }

        String group = hash.substring(0, nPrefGroup);
        String subGroup = hash.substring(0, nPrefSubGroup);

        // Put it in the store. Note that this causes our secondary key
        // to be automatically updated for us.
        Blob blob = new Blob(hash, group, subGroup, bytes);
        dbAccessor.getBlobByIndex().putNoOverwrite(blob);

        // When insert a blob, insert the blob in one table and also insert into a key table.
        // Where the key table has the group and the blob's hash. If doesn't exist, 
        // we just insert to DB with 'INVALID' status and should delete that root group 
        // and sub group because the original root group and sub group are no longer valid
        dbAccessor.getGroupByIndex().put(new HashGroup(group, null, Status.INVALID));
        dbAccessor.getSubGroupByIndex().put(new SubGroup(subGroup, group, null, Status.INVALID));
        lastCommit = new Date();
        doCommit = true;
        commitCount++;
    }

    @Override
    public byte[] getBlob(String hash) {
        if (hash == null) {
            throw new RuntimeException("Key can not be null for get blob function");
        }

        Blob blob = dbAccessor.getBlobByIndex().get(hash);
        if (blob == null) {
            return null;
        }

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
     * E.g +-----------+---------------+---------------+ | NAME | CONTENT |
     * STATUS | +-----------+---------------+---------------+ | 012 | xxxxxx |
     * INVALID | | ccc | xxxxxx | INVALID | | xyz | xxxxxx | INVALID | | abc |
     * xxxxxx | INVALID | +-----------+---------------+---------------+
     *
     */
    public void generateHashes() {
        try (EntityCursor<HashGroup> entities = dbAccessor.getGroupByStatus().subIndex(Status.INVALID).entities()) {
            for (HashGroup hashGroup : entities) {
                List<HashGroup> subGroups = getSubGroups(hashGroup.getName());

                String recalHash = Crypt.toHexFromBlob(subGroups);
                hashGroup.setContentHash(recalHash);
                hashGroup.setStatus(Status.VALID);
                dbAccessor.getGroupByIndex().put(hashGroup);
            }
        }
    }

    /**
     * Get the group hashes for the initial hash prefix (ie first 3 chars).
     * Return only those currently persisted ie do not dynamically generate any
     * missing hashes
     *
     * E.g	+-----------+---------------+---------------+ |	NAME	|	CONTENT	|
     * STATUS	| +-----------+---------------+---------------+ |	012 |	xxxxxx	|
     * VALID	| |	ccc |	xxxxxx	|	VALID	| |	xyz |	xxxxxx	|	VALID	| |	abc |	xxxxxx
     * |	VALID	| +-----------+---------------+---------------+
     *
     * @return
     */
    public List<HashGroup> getRootGroups() {
        List<HashGroup> groups = new ArrayList<>();
        try (EntityCursor<HashGroup> entities = dbAccessor.getGroupByStatus().subIndex(Status.VALID).entities()) {
            Iterator<HashGroup> iterator = entities.iterator();
            if (iterator instanceof List) {
                return (List<HashGroup>) iterator;
            }

            if (iterator != null) {
                while (iterator.hasNext()) {
                    groups.add(iterator.next());
                }
            }
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
        List<HashGroup> groups = new ArrayList<>();
        try (EntityCursor<SubGroup> entities = dbAccessor.getSubGroupByParent().subIndex(parent).entities()) {
            for (SubGroup subGroup : entities) {
                if (subGroup.getStatus().equals(Status.INVALID)) {
                    getBlobHashes(subGroup.getName());
                    subGroup = dbAccessor.getSubGroupByIndex().get(subGroup.getName());
                }

                HashGroup hashGroup = new HashGroup(subGroup.getName(),
                        subGroup.getContentHash(), subGroup.getStatus());
                groups.add(hashGroup);
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
        List<String> hashes = new ArrayList<>();
        try (EntityCursor<Blob> entities = dbAccessor.getBlobBySubGroup().subIndex(subGroupName).entities()) {
            for (Blob blob : entities) {
                hashes.add(blob.getHash());
            }

            String recalHash = Crypt.toHexFromHash(hashes);
            String rootGroup = subGroupName.substring(0, nPrefGroup);
            SubGroup subGroup = new SubGroup(subGroupName, rootGroup, recalHash, Status.VALID);
            dbAccessor.getSubGroupByIndex().put(subGroup);
        }
        return hashes;
    }

    private void writeToDisk() {
        Date now = new Date();
        if ((lastCommit == null || (now.getTime() - lastCommit.getTime()) > 5000 || commitCount > 10000) && doCommit) {
            this.dbEnv.getEnv().sync();
            doCommit = false;
            commitCount = 0;
        }
    }

    /**
     * 
     * @param hash
     * @param bytes 
     */
    @Override
    public void pushBlob(String hash, byte[] bytes) {
        if(!hasBlob(hash)){
            setBlob(hash, bytes);
        }
    }
}