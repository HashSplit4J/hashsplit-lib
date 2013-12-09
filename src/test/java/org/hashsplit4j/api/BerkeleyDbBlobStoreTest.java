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

import static org.junit.Assert.*;

import java.io.File;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 *
 * @version BerkeleyStoreTest.java Dec 5, 2013
 */
public class BerkeleyDbBlobStoreTest {

    Charset CHARSET_UTF = Charset.forName("UTF-8");

    BerkeleyDbBlobStore blobStore;

    File dbDir;

    long cacheSize = 20 * 1024 * 1024;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        dbDir = new File("target/data");

        blobStore = new BerkeleyDbBlobStore(dbDir, cacheSize);
    }

    @After
    public void tearDown() {
        blobStore.close();
        for( File f : dbDir.listFiles() ) {
            if( !f.delete() ) {
                System.out.println("Couldnt delete: " + f.getAbsolutePath());
            }
        }
        if (!dbDir.delete()) {
            System.out.println("Failed to delete db directory");
        }
    }

    @Test
    public void testSetBlobWithoutDuplicate() {
        // Insert 10000 entities to berkeley
        for (int j = 10000; j >= 0; j--) {
            String data = String.valueOf(j);

            blobStore.setBlob(DigestUtils.shaHex(data), data.getBytes(CHARSET_UTF));
        }
        
        blobStore.getRootGroups();
    }

    @Test
    public void testSetBlobWithDuplicate() {
        String origData = "1";
        String key = DigestUtils.shaHex("1");
        blobStore.setBlob(key, origData.getBytes(CHARSET_UTF));
        
        String newData = "Oracle Berkeley DB Java Edition";

        // Try to overwrite the entity {key: "1", value: "Oracle Berkeley DB Java Edition"}
        blobStore.setBlob(key, newData.getBytes(CHARSET_UTF));

        // The berkeley should keep original data like this entity {key: "1", value: "1"}
        String expResult = new String(blobStore.getBlob(key), CHARSET_UTF);

        assertEquals("1", expResult);
    }

    @Test
    public void testGetBlob() {
        String actualData = "10000";
        String key = DigestUtils.shaHex(actualData);
        
        blobStore.setBlob(key, actualData.getBytes(CHARSET_UTF));
        
        byte[] data = blobStore.getBlob(key);
        assertNotNull(data);
        String expData = new String(data, CHARSET_UTF);

        assertEquals("10000", expData);
    }

    /**
     * Test hasBlob when there is a blob
     */
    @Test
    public void testHasBlobWithExist() {
        String key = DigestUtils.shaHex("10");
        blobStore.setBlob(key, "XXX".getBytes(CHARSET_UTF));
        // Found a Blob
        assertTrue(blobStore.hasBlob(key));
    }

    /**
     * Test hasBlob when there is not a blob
     */
    @Test
    public void testHasBlobWithoutExist() {
        String key = DigestUtils.shaHex("20000");

        // Not found a Blob
        assertFalse(blobStore.hasBlob(key));
    }
    
    @Test
    public void testGroupCreation() {
        System.out.println("testSetBlobCreatesGroup");
        String data = "20000";
        byte[] bytes = data.getBytes(CHARSET_UTF);
        String key = "352bc7d47decfa6b5052a0dd871ef73d6a91c7de";
        System.out.println("key: " + key);
        String rootGroupName = key.substring(0, 3);
        String subGroupName = key.substring(0, 6);
        System.out.println("rootGroupName: " + rootGroupName);
        System.out.println("subGroupName: " + subGroupName);
        // Check the group is not present
        List<BerkeleyDbBlobStore.HashGroup> rootGroups = blobStore.getRootGroups();
        assertTrue(rootGroups == null || rootGroups.isEmpty());
        
        // ok, nothing in there, lets do an insert and check for one group
        blobStore.setBlob(key, bytes);
        
        // Should not have generated any hash groups yet
        rootGroups = blobStore.getRootGroups();
        assertEquals(0, rootGroups.size()); // should be NO root groups yet
        
        // Lets generate the hash groups
        blobStore.generateHashes();

        // OK, there should now be a root group corresponding to rootGroupName above
        rootGroups = blobStore.getRootGroups();
        assertEquals(1, rootGroups.size()); // should be 1 root group
        assertEquals(rootGroupName, rootGroups.get(0).getName()); // the name of that group should be first 3 chars of the key
        
        // Now lets drill down, should be a single subgroup for that rootgroup
        List<BerkeleyDbBlobStore.HashGroup> subGroups = blobStore.getSubGroups(rootGroupName);
        assertNotNull(subGroups);
        assertEquals(1, subGroups.size()); // should be one entry
        assertEquals(subGroupName, subGroups.get(0).getName()); // the name of this group should be the subgroup name, ie first 6 chars
        
        // Now we have a subgroup, we can load blobs for that group
        List<String> blobHashes = blobStore.getBlobHashes(subGroupName);
        assertNotNull(blobHashes);
        assertEquals(1, blobHashes.size()); // should be one entry
        String actualBlobHash = blobHashes.get(0);
        assertEquals(key, actualBlobHash);               
    }
    
    @Test
    public void testSetBlobRemovesGroup() {
        // Insert a blob and generate hash groups. Then insert another blob with same root group and ensure
        // the root group gets deleted
        String data = "20000";
        byte[] bytes = data.getBytes(CHARSET_UTF);
        String key1 = "352bc7d47decfa6b5052a0dd871ef73d6a91c7de";
        String key2 = "352000007decfa6b5052a0dd871ef73d6a91c7de";
        
        blobStore.setBlob(key1, bytes);
        blobStore.generateHashes();
        List<BerkeleyDbBlobStore.HashGroup> rootGroups = blobStore.getRootGroups();
        assertEquals(1, rootGroups.size());
        
        // Now insert a blob with a matching root group name. This should delete that root group
        // because the original root group is no longer valid
        blobStore.setBlob(key2, bytes);
        rootGroups = blobStore.getRootGroups();
        assertEquals(0, rootGroups.size());
        
        // Now generate
        blobStore.generateHashes();
        
        rootGroups = blobStore.getRootGroups();
        assertEquals(1, rootGroups.size());
    }    
}
