package org.hashsplit4j.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BerkeleyDbBlobStoreImportTest {

    BerkeleyDbBlobStore blobStore;

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
    public void testImport() {
        assertEquals(0, blobStore.getRootGroups()); // just make sure starting with empty db
        
        File dir = new File("src/test/resources/import-test");
        
        blobStore.importFiles(dir);
        
        assertEquals(5, blobStore.getRootGroups()); // 1c, 2b, 2c, 2d, 2e, 5b
    }
    
}
