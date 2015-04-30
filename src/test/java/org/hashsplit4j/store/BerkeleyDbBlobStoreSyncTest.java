package org.hashsplit4j.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;
import org.hashsplit4j.api.HashGroup;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BerkeleyDbBlobStoreSyncTest {

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
    }   */
}
