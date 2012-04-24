package org.hashsplit4j.api;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author brad
 */
public class DownloadSyncSimulation {
    /**
     * Simulates this scenario:
     *  - user has an unmodified local file platypus.bmp
     *  - the server has a modified version (platypus-mod.bmp)
     *  - user wants to update her local copy to be the same as the server copy
     *  - we want to minimised "network" traffic, so use blobs from current file as much as possible
     * 
     * For this test case we don't want to change our test data to the rebuilt
     * file will not overwrite platypus.bmp
     * 
     * We will parse the "server" file in this test, although in practise the server
     * would already have that information in a database etc
     * 
     */
    @Test
    public void testDownSync() throws IOException {
        MemoryBlobStore serverBlobStore = new MemoryBlobStore();
        MemoryHashStore serverHashStore = new MemoryHashStore();
        InputStream serverFileData = Scratch.class.getResourceAsStream("platypus-mod.bmp");
        Parser parser = new Parser();
        long fileHash = parser.parse(serverFileData, serverHashStore, serverBlobStore);
        List<Long> serverFanouts = serverHashStore.getFanout(fileHash).getHashes();
        // so we now have the equivalent of a server
        
        MemoryHashStore clientHashStore = new MemoryHashStore();
        File clientOrigFile = new File("src/test/resources/org/hashsplit4j/api/platypus.bmp");
        FileBlobStore clientBlobStore = new FileBlobStore(clientOrigFile);
        FileInputStream clientFileData = new FileInputStream(clientOrigFile);
        parser.parse(clientFileData, clientHashStore, clientBlobStore);
        clientBlobStore.openForRead(); // prepare local file for reading blobs
        
        // ok so now we have client and server all ready, lets pretend to combine them
        File clientUpdatedFile = new File("target/platypus-updated.bmp");
        if( clientUpdatedFile.exists() ) {
            clientUpdatedFile.delete(); // delete if still there from last test run
        }
        FileOutputStream fout = new FileOutputStream(clientUpdatedFile);
        
        // to build updated local file, we ge the fanout crc's from the server
        // and iterate over the contained chunks. For each chunk we try to get
        // a blob from the local copy if possible, and only go to server for blobs
        // that we don't have locally
        
        MultipleBlobStore multipleBlobStore = new MultipleBlobStore(Arrays.asList(clientBlobStore, serverBlobStore)); // use client preferentially to server to minimise network
        MultipleHashStore multipleHashStore = new MultipleHashStore(Arrays.asList(clientHashStore, serverHashStore));
        Combiner combiner = new Combiner(); 
        combiner.combine(serverFanouts, multipleHashStore, multipleBlobStore, fout);
        clientBlobStore.close(); // close the local file blob store
        fout.flush();                
        fout.close();
        assertEquals(218870, clientUpdatedFile.length());        
    }
}
