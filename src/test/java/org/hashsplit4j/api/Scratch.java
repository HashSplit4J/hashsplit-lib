package org.hashsplit4j.api;

import java.io.*;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 *
 * @author brad
 */
public class Scratch {

    //@Test
    public void bitmapTest() throws IOException {
        Map<Long, MemoryBlobStore.Chunk> map1 = processFile("platypus.bmp", 218870);
        Map<Long, MemoryBlobStore.Chunk> map2 = processFile("platypus-mod.bmp", 218870);
        checkCommon(map1, map2, 80);
    }

    //@Test
    public void wordDocTest() throws IOException {
        Map<Long, MemoryBlobStore.Chunk> map1 = processFile("doc1.rtf", 57911);
        Map<Long, MemoryBlobStore.Chunk> map2 = processFile("doc2.rtf", 57914); // Just has a few extra bytes at start
        checkCommon(map1, map2, 80);
    }

    @Test
    public void testFileBlobStore() throws IOException {
        MemoryHashStore hashStore = new MemoryHashStore();
        File file = new File("src/test/resources/org/hashsplit4j/api/test1.txt");
        FileBlobStore blobStore = new FileBlobStore(file);
        Parser parser = new Parser();
        FileInputStream in = new FileInputStream(file);
        long fileHash = parser.parse(in, hashStore, blobStore);
        in.close();
        assertEquals(file.length(), blobStore.getTotalSize()); // check reconstituted size is same as the file

        blobStore.openForRead();
        Combiner combiner = new Combiner();
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        Fanout fileFanout = hashStore.getFanout(fileHash);
        combiner.combine(fileFanout.getHashes(), hashStore, blobStore, bout);
        System.out.println("re-constitued size: " + bout.size());
        assertEquals(file.length(), bout.size()); // check reconstituted size is same as the file        
        blobStore.close();
    }

    private void checkCommon(Map<Long, MemoryBlobStore.Chunk> map1, Map<Long, MemoryBlobStore.Chunk> map2, int minPercent) {
        System.out.println("Check common blobs");
        // find out how many we have in common
        int common = 0;
        for (Long l : map1.keySet()) {
            if (map2.containsKey(l)) {
                common++;
            }
        }
        int pc = common * 100 / map1.size();
        System.out.println("Common blobs: " + common);
        System.out.println("Total blobs: " + map1.size());
        System.out.println("Common data: " + pc + "%");
        assertTrue("Must be at least " + minPercent + "% in common. Is " + pc + "%", pc > minPercent); // check at least 80% is common
    }

    public Map<Long, MemoryBlobStore.Chunk> processFile(String fname, long size) throws IOException {
        InputStream in = Scratch.class.getResourceAsStream(fname);
        //InputStream in = Scratch.class.getResourceAsStream("test1.txt");
        MemoryHashStore hashStore = new MemoryHashStore();
        MemoryBlobStore blobStore = new MemoryBlobStore();
        Parser parser = new Parser();
        long fileHash = parser.parse(in, hashStore, blobStore);
        assertEquals(size, blobStore.getTotalSize()); // check reconstituted size is same as the file

        System.out.println("-----------------------------------");
        System.out.println("---------- Restore file -------------");
        Combiner combiner = new Combiner();
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        Fanout fileFanout = hashStore.getFanout(fileHash);
        combiner.combine(fileFanout.getHashes(), hashStore, blobStore, bout);
        System.out.println("re-constitued size: " + bout.size());
        assertEquals(size, bout.size()); // check reconstituted size is same as the file

        System.out.println("Final stats");
        System.out.println("Num blobs: " + blobStore.getMapOfChunks().size());
        System.out.println("Num fanouts: " + hashStore.getNumFanouts());
        return blobStore.getMapOfChunks();
    }
}
