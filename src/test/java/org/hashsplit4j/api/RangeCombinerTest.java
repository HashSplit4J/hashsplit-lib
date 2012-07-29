package org.hashsplit4j.api;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author brad
 */
public class RangeCombinerTest {

    MemoryBlobStore blobStore;
    MemoryHashStore hashStore;
    InputStream serverFileData;
    List<Long> serverFanouts;
    
    @Before
    public void setup() throws IOException {
        blobStore = new MemoryBlobStore();
        hashStore = new MemoryHashStore();
        serverFileData = Scratch.class.getResourceAsStream("platypus-mod.bmp");
        Parser parser = new Parser();
        long fileHash = parser.parse(serverFileData, hashStore, blobStore);
        serverFanouts = hashStore.getFanout(fileHash).getHashes();        
    }
    
    
    @Test
    public void test_SmallRange() throws IOException {
        Combiner combiner = new Combiner();
        
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        combiner.combine(serverFanouts, hashStore, blobStore, bout, 100l, 199l);
        
        assertEquals(100, bout.size());
    } 
    
    @Test
    public void test_LargeRange() throws IOException {
        Combiner combiner = new Combiner();
        
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        combiner.combine(serverFanouts, hashStore, blobStore, bout, 100000l, 199999l);
        
        assertEquals(100000, bout.size());
    } 
    
    @Test
    public void test_ZeroStart() throws IOException {
        Combiner combiner = new Combiner();
        
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        combiner.combine(serverFanouts, hashStore, blobStore, bout, 0, 199l);
        
        assertEquals(200, bout.size());
    }      
    
    @Test
    public void test_Finish() throws IOException {
        Combiner combiner = new Combiner();
        
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        combiner.combine(serverFanouts, hashStore, blobStore, bout, (218870-20000), null); // size is 218870
        
        assertEquals(20000, bout.size());
    }     
}
