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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;
import org.hashsplit4j.api.Crypt;
import org.hashsplit4j.api.HashGroup;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 *
 * @version BerkeleyStoreTest.java Dec 5, 2013
 */
public class BerkeleyDbBlobStoreTest {
    
    /*BerkeleyDbBlobStore blobStore;

    File envHome;
    
    int nPrfGroup = 3;
    int nPrfSubGroup = 6;

    @Before
    public void setUp() throws Exception {
        envHome = new File("target/data");
        blobStore = new BerkeleyDbBlobStore(envHome, nPrfGroup, nPrfSubGroup);
    }

    @After
    public void tearDownAfterClass() throws Exception {
    	blobStore.closeEnv();
        blobStore.removeDbFiles(envHome);
    }

    @Test
    public void testSetBlobWithoutDuplicate() {
        String data = "1";
        String hash = Crypt.toHexFromText(data);
        
        blobStore.setBlob(hash, data.getBytes());
        byte[] expected = blobStore.getBlob(hash);
        
        assertEquals(data, new String(expected));
    }

    @Test
    public void testSetBlobWithDuplicate() {
        String origData = "1";
        String hash = Crypt.toHexFromText("1");
        blobStore.setBlob(hash, origData.getBytes());
        
        String newData = "Oracle Berkeley DB Java Edition";
        blobStore.setBlob(hash, newData.getBytes());

        String expResult = new String(blobStore.getBlob(hash));
        assertEquals(origData, expResult);
    }

    @Test
    public void testGetBlob() {
        String actualData = "This is value of the key 1000:";
        String hash = Crypt.toHexFromText("1000");
        
        blobStore.setBlob(hash, actualData.getBytes());
        
        byte[] data = blobStore.getBlob(hash);
        assertNotNull(data);
        String expData = new String(data);

        assertEquals(actualData, expData);
    }*/

    /**
     * Test hasBlob when there is a blob
     */
    /*@Test
    public void testHasBlobWithExist() {
        String hash = Crypt.toHexFromText("10");
        blobStore.setBlob(hash, "XXX".getBytes());
        
        // Found a Blob
        assertTrue(blobStore.hasBlob(hash));
    }*/

    /**
     * Test hasBlob when there is not a blob
     */
    /*@Test
    public void testHasBlobWithoutExist() {
        String hash = Crypt.toHexFromText("1001");

        // Not found a Blob
        assertFalse(blobStore.hasBlob(hash));
    }*/
    
    /**
     * There is not a hash group if didn't call generated hashes function
     */
    /*@Test
    public void testHashGroupsWithoutGenerateHashes() {
        // Should not have generated any hash groups yet
        List<HashGroup> rootGroups = blobStore.getRootGroups();
        assertEquals(0, rootGroups.size()); // should be NO root groups yet
    }*/
    
    /**
     * There are 892 hash group when call generated hashes function
     * 
     */
    /*@Test
    public void testHashGroupsWithGenerateHashes() {
        String hash = Crypt.toHexFromText("1");
        blobStore.setBlob(hash, "1".getBytes());
        
        hash = Crypt.toHexFromText("10");
        blobStore.setBlob(hash, "XXX".getBytes());
        
        hash = Crypt.toHexFromText("1000");
        blobStore.setBlob(hash, "This is value of the key 1000:".getBytes());
        
        List<HashGroup> rootGroups = blobStore.getRootGroups();
        // Should not have generated any hash groups yet
        assertEquals(0, rootGroups.size());
        
        // Lets generate the hash groups
        blobStore.generateHashes();
        
        // OK, there should now be 3 root group corresponding to rootGroupName above
        rootGroups = blobStore.getRootGroups();
        assertEquals(3, rootGroups.size()); // should be 3 root groups
    }
    
    @Test
    public void testSubGroupWithParent() {
        String hash = "f739349daff6e29994b561a6d402f4ebea8f7edb";
        String data = "Berkeley DB Java Edition";
        
        blobStore.setBlob(hash, data.getBytes());
      
        String parent = "f73";
        System.out.println("\tGet all sub group in the group " + parent);
        System.out.println("--------------------------------------------------------");
        List<HashGroup> subGroups = blobStore.getSubGroups(parent);
        for (HashGroup group : subGroups) {
            System.out.println("Group's name: " + group.getName());
            System.out.println("Content hash: " + group.getContentHash());
        }
        
        assertEquals(1, subGroups.size());
    }
    
    @Test
    public void testBlobHashes() {
    	String hash = "f739349daff6e29994b561a6d402f4ebea8f7edb";
    	// Put to berkeley-db
        blobStore.setBlob(hash, "Berkeley DB Java Edition".getBytes());
        
        String subGroup = "f73934";
        // Find all blob for the give sub group's name
        List<String> hashes = blobStore.getBlobHashes(subGroup);
        
        // Should have ONE hash f739349daff6e29994b561a6d402f4ebea8f7edb
        assertEquals(1, hashes.size());
    }*/
}
