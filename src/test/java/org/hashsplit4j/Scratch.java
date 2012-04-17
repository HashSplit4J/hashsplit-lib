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
    public void bitmapTest() throws IOException {
        Map<Long, byte[]> map1 = processFile("platypus.bmp", 218870);
        Map<Long, byte[]> map2 = processFile("platypus-mod.bmp", 218870);
        checkCommon( map1, map2, 80); 
    }

    @Test
    public void wordDocTest() throws IOException {
        Map<Long, byte[]> map1 = processFile("doc1.rtf", 57911);                
        Map<Long, byte[]> map2 = processFile("doc2.rtf", 57914); // Just has a few extra bytes at start
        checkCommon( map1, map2, 80); 
    }
    
    
    private void checkCommon(Map<Long, byte[]> map1, Map<Long, byte[]> map2, int minPercent) {
        System.out.println("Check common blobs");
        // find out how many we have in common
        int common = 0;
        for( Long l : map1.keySet()) {
            if( map2.containsKey(l)) {
                common++;
            }
        }
        int pc = common * 100/map1.size();
        System.out.println("Common blobs: " + common);
        System.out.println("Total blobs: " + map1.size());        
        System.out.println("Common data: " + pc + "%");
        assertTrue("Must be at least " + minPercent + "% in common. Is " + pc + "%",  pc > minPercent ); // check at least 80% is common
    }
    
    public Map<Long,byte[]> processFile(String fname, long size) throws IOException {
        InputStream in = Scratch.class.getResourceAsStream(fname);
        //InputStream in = Scratch.class.getResourceAsStream("test1.txt");
        MemoryHashStore store = new MemoryHashStore();
        Parser parser = new Parser();
        List<Long> megaCrcs = parser.parse(in, store);
        assertEquals(size, store.getTotalSize()); // check reconstituted size is same as the file
        
        System.out.println("-----------------------------------");
        System.out.println("---------- Restore file -------------");
        Combiner combiner = new Combiner();
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        combiner.combine(megaCrcs, store, bout);
        System.out.println("re-constitued size: " + bout.size());
        assertEquals(size, bout.size()); // check reconstituted size is same as the file
        
        System.out.println("Final stats");
        System.out.println("Num blobs: " + store.getNumBlobs());
        System.out.println("Num chunks: " + store.getNumChunks());
        System.out.println("Num fanouts: " + store.getNumFanouts());
        return store.getMapOfBlobs();
    }
}
