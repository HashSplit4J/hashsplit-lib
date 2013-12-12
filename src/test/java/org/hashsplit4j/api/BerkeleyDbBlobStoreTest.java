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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

/**
 *
 *
 * @version BerkeleyStoreTest.java Dec 5, 2013
 */
public class BerkeleyDbBlobStoreTest {
    
    static BerkeleyDbBlobStore blobStore;

    static File envHome;
    
    int nPrfGroup = 3;
    int nPrfSubGroup = 6;

    @Before
    public void setUp() throws Exception {
        envHome = new File("target/data");
        blobStore = new BerkeleyDbBlobStore(envHome, nPrfGroup, nPrfSubGroup);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        blobStore.closeEnv();
        if (envHome.isDirectory()) {
            for(File f : envHome.listFiles()) {
                if( !f.delete() ) {
                    System.err.println("Couldnt delete: " + f.getAbsolutePath());
                }
            }
        }
        
        if (!envHome.delete()) {
            System.err.println("Failed to delete db directory");
        }
    }

//    @Test
    public void testSetBlobWithoutDuplicate() {
        // Insert 1000 entities to berkeleydb
        for (int j = 1000; j >= 0; j--) {
            String data = "This is value of the key " + j + ":";
            String hash = Crypt.toHexFromText(String.valueOf(j));
            blobStore.setBlob(hash, data.getBytes());
        }
    }

//    @Test
    public void testSetBlobWithDuplicate() {
        String origData = "This is value of the key 1:";
        String hash = Crypt.toHexFromText("1");
        blobStore.setBlob(hash, origData.getBytes());
        
        String newData = "Oracle Berkeley DB Java Edition";
        blobStore.setBlob(hash, newData.getBytes());

        String expResult = new String(blobStore.getBlob(hash));
        assertEquals(origData, expResult);
    }

//    @Test
    public void testGetBlob() {
        String actualData = "This is value of the key 1000:";
        String hash = Crypt.toHexFromText("1000");
        
        blobStore.setBlob(hash, actualData.getBytes());
        
        byte[] data = blobStore.getBlob(hash);
        assertNotNull(data);
        String expData = new String(data);

        assertEquals(actualData, expData);
    }

    /**
     * Test hasBlob when there is a blob
     */
//    @Test
    public void testHasBlobWithExist() {
        String hash = Crypt.toHexFromText("10");
        blobStore.setBlob(hash, "XXX".getBytes());
        
        // Found a Blob
        assertTrue(blobStore.hasBlob(hash));
    }

    /**
     * Test hasBlob when there is not a blob
     */
//    @Test
    public void testHasBlobWithoutExist() {
        String hash = Crypt.toHexFromText("1001");

        // Not found a Blob
        assertFalse(blobStore.hasBlob(hash));
    }
    
    /**
     * There is not a hash group if didn't call generated hashes function
     */
//    @Test
    public void testHashGroupsWithoutGenerateHashes() {
        // Should not have generated any hash groups yet
        List<HashGroup> rootGroups = blobStore.getRootGroups();
        assertEquals(0, rootGroups.size()); // should be NO root groups yet
    }
    
    /**
     * There are 892 hash group when call generated hashes function
     * 
     */
//    @Test
    public void testHashGroupsWithGenerateHashes() {
        // Should not have generated any hash groups yet
        List<HashGroup> rootGroups = blobStore.getRootGroups();
        
        // Lets generate the hash groups
        blobStore.generateHashes();
        
        // OK, there should now be 892 root group corresponding to rootGroupName above
        rootGroups = blobStore.getRootGroups();
        assertEquals(892, rootGroups.size()); // should be 892 root groups
    }
    
//    @Test
    public void testSubGroupWithParent() {
        String parent = "f73";
        System.out.println("\tGet all sub group in the group " + parent);
        System.out.println("--------------------------------------------------------");
        List<HashGroup> subGroups = blobStore.getSubGroups(parent);
        for (HashGroup group : subGroups) {
            System.out.println("Group's name: " + group.getName());
            System.out.println("Content hash: " + group.getContentHash());
        }
        
        assertEquals(2, subGroups.size());
    }
    
//    @Test
    public void testBlobHashes() {
        String subGroup = "f73934";
        System.out.println("\tGet all blobs in the sub group " + subGroup);
        System.out.println("--------------------------------------------------------");
        List<String> hashes = blobStore.getBlobHashes(subGroup);
        for (String hash : hashes) {
            System.out.println("Hash: " + hash);
        }
        
        // Should have ONE hash f739349daff6e29994b561a6d402f4ebea8f7edb
        assertEquals(1, hashes.size());
    }
    
    @Test
    public void testGroupCreation() {
        System.out.println("\tTest Group Creation");
        System.out.println("--------------------------------------------------------");
        String data = "20000";
        byte[] bytes = data.getBytes();
        String key = "352bc7d47decfa6b5052a0dd871ef73d6a91c7de";
        System.out.println("key: " + key);
        String rootGroupName = key.substring(0, 3);
        String subGroupName = key.substring(0, 6);
        System.out.println("Group's name: " + rootGroupName);
        System.out.println("SubGroups's name: " + subGroupName);
        // Check the group is not present
        List<HashGroup> rootGroups = blobStore.getRootGroups();
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
        List<HashGroup> subGroups = blobStore.getSubGroups(rootGroupName);
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
        byte[] bytes = data.getBytes();
        String key1 = "352bc7d47decfa6b5052a0dd871ef73d6a91c7de";
        String key2 = "352000007decfa6b5052a0dd871ef73d6a91c7de";
        
        blobStore.setBlob(key1, bytes);
        blobStore.generateHashes();
        List<HashGroup> rootGroups = blobStore.getRootGroups();
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
