package org.hashsplit4j.api;

import org.hashsplit4j.api.MemoryBlobStore;
import org.hashsplit4j.api.UpdatingBlobStore;
import org.hashsplit4j.api.MemoryHashStore;
import org.hashsplit4j.api.UpdatingHashStore;
import org.hashsplit4j.api.Parser;
import java.io.*;
import java.util.List;
import org.junit.Test;

/**
 *
 * @author brad
 */
public class UploadSyncSimulation {
    /**
     * Simulates this scenario:
     * - server contains original file platypus.bmp
     * - user has a modified version (platypus-mod.bmp) which they want to upload
     * 
     */
    @Test
    public void testUploadSync() throws FileNotFoundException, IOException {
        Parser parser = new Parser();
                
        // prepare the "server"
        MemoryBlobStore serverBlobStore = new MemoryBlobStore();
        MemoryHashStore serverHashStore = new MemoryHashStore();
        InputStream serverFileData = Scratch.class.getResourceAsStream("platypus.bmp");
        parser.parse(serverFileData, serverHashStore, serverBlobStore);
        
        // parse the client file
        System.out.println("------------------------------------------------------");
        System.out.println("----- Now process file and send changes to server ------");
        UpdatingHashStore clientHashStore = new UpdatingHashStore(serverHashStore);
        File clientModFile = new File("src/test/resources/org/hashsplit4j/api/platypus-mod.bmp");
        UpdatingBlobStore updatingBlobStore = new UpdatingBlobStore(serverBlobStore); // blobs will be sent to the server store
        FileInputStream clientFileData = new FileInputStream(clientModFile);
        parser.parse(clientFileData, clientHashStore, updatingBlobStore);        
        // Now send data to the server
        
        
        
    }
}
