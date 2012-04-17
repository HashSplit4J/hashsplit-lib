package org.hashsplit4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 *
 * @author brad
 */
public class Scratch {
    
    
    @Test
    public void scratchTest() throws IOException {
        Map<Long, byte[]> map1 = processFile("platypus.bmp");
        Map<Long, byte[]> map2 = processFile("platypus-mod.bmp");
        
        // find out how many we have in common
        int common = 0;
        for( Long l : map1.keySet()) {
            if( map2.containsKey(l)) {
                common++;
            }
        }
        System.out.println("Common blobs: " + common);
        System.out.println("Total blobs: " + map1.size());
        assertTrue( common * 100/map1.size() > 80 ); // check at least 80% is common
    }
    
    public Map<Long,byte[]> processFile(String fname) throws IOException {
        InputStream in = Scratch.class.getResourceAsStream(fname);
        //InputStream in = Scratch.class.getResourceAsStream("test1.txt");
        MemoryHashStore store = new MemoryHashStore();
        Parser parser = new Parser();
        List<Long> megaCrcs = parser.parse(in, store);
        assertEquals(218870, store.getTotalSize()); // check reconstituted size is same as the file
        
        System.out.println("-----------------------------------");
        System.out.println("---------- Restore file -------------");
        Combiner combiner = new Combiner();
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        combiner.combine(megaCrcs, store, bout);
        System.out.println("re-constitued size: " + bout.size());
        assertEquals(218870, bout.size()); // check reconstituted size is same as the file
        
        System.out.println("Final stats");
        System.out.println("Num blobs: " + store.getNumBlobs());
        System.out.println("Num chunks: " + store.getNumChunks());
        System.out.println("Num fanouts: " + store.getNumFanouts());
        return store.getMapOfBlobs();
    }
}
