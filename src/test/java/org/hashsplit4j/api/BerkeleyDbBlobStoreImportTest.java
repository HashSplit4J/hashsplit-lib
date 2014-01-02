package org.hashsplit4j.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
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
    public void testImportted() throws FileNotFoundException {
        File dir = new File("src/test/resources/import-test");
        assertTrue(dir.exists());
        assertTrue(dir.isDirectory());
        
        int importted = blobStore.importFiles(dir);
        assertEquals(7, importted);
    }
    
    @Test
    public void testImporttedHasBlob() throws FileNotFoundException {
    	File dir = new File("src/test/resources/import-test");
        assertTrue(dir.exists());
        assertTrue(dir.isDirectory());
        
        int importted = blobStore.importFiles(dir);
        assertEquals(7, importted);
        
        String hash = "1c8e930f68f4c260760e0d2e238e905a978e4259";
        assertTrue(blobStore.hasBlob(hash));
        
        hash = "1cf8d9a9824c83b082565eb8d2d79e9dd264d7b9";
        assertTrue(blobStore.hasBlob(hash));
        
        hash = "2bfe75ca2e7e5325d8b7e87c4a71f348964f2604";
        assertTrue(blobStore.hasBlob(hash));
        
        hash = "2cb11961d5eefc86620667c561945f73a87d5129";
        assertTrue(blobStore.hasBlob(hash));
        
        hash = "2d01664493f1a3ec7225bff62b12630f63296f67";
        assertTrue(blobStore.hasBlob(hash));
        
        hash = "2e2f1a2521269422d1b6a501114851cea08ed652";
        assertTrue(blobStore.hasBlob(hash));
        
        hash = "5b8ddd0ef0d184b6958b0526ced423fb74b70fc3";
        assertTrue(blobStore.hasBlob(hash));
    }
    
    @Test
    public void testImporttedGetBlob() throws IOException {
        File dir = new File("src/test/resources/import-test");
        assertTrue(dir.exists());
        assertTrue(dir.isDirectory());
        
        int importted = blobStore.importFiles(dir);
        assertEquals(7, importted);
        
        String hash = "1c8e930f68f4c260760e0d2e238e905a978e4259";
        File file = new File("src/test/resources/import-test/1c8e93/0f68f4/c26076/0e0d2e/238e90/5a978e/1c8e930f68f4c260760e0d2e238e905a978e4259");
        byte[] contents = FileUtils.readFileToByteArray(file);
        
        String actualContents = new String(blobStore.getBlob(hash));
        String expertContents = new String(contents);
        assertEquals(expertContents, actualContents);
        
        System.out.println("\t\tActually Contents:");
        System.out.println("***********************************************************");
        System.out.println(actualContents);
        System.out.println("***********************************************************");
    }

    @Test
    public void testImportFiles() throws FileNotFoundException {
        
        assertEquals(0, blobStore.getRootGroups().size()); // just make sure starting with empty db
        
        File dir = new File("src/test/resources/import-test");
        System.out.println("importing from: " + dir.getAbsolutePath());
        assertTrue(dir.exists());
        assertTrue(dir.isDirectory());
        
        blobStore.importFiles(dir);
        // Should not have generated any hash groups yet
        assertEquals(0, blobStore.getRootGroups().size());
        
        // Now generate
        blobStore.generateHashes();
        
        assertEquals(7, blobStore.getRootGroups().size()); // 1c8, 1cf, 2bf, 2cb, 2d0, 2e2, 5b8
    }
}
