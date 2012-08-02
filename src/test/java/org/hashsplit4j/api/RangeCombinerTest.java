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
    String fileHash;
    InputStream serverFileData;
    List<String> serverFanouts;

    @Before
    public void setup() throws IOException {
        System.out.println("Setup");
        blobStore = new MemoryBlobStore();
        hashStore = new MemoryHashStore();
        serverFileData = Scratch.class.getResourceAsStream("platypus-mod.bmp");
        Parser parser = new Parser();
        System.out.println("parse---");
        fileHash = parser.parse(serverFileData, hashStore, blobStore);
        System.out.println("done parse");
        serverFanouts = hashStore.getFileFanout(fileHash).getHashes();
        System.out.println("serverFanouts size: " + serverFanouts.size());
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

        // Show fanouts and blobs:
        System.out.println("Show fanouts/blobs for: " + fileHash);
        for (String fanoutHash : serverFanouts) {
            Fanout fanout = hashStore.getChunkFanout(fanoutHash);
            System.out.println("Mega: " + fanoutHash + " size: " + fanout.getHashes().size());
            for (String blobHash : fanout.getHashes()) {
                byte[] b = blobStore.getBlob(blobHash);
                if( b == null ) {
                    fail("Could not find blob: " + blobHash);
                }
                System.out.println("        Blob: " + blobHash + " suze: " + b.length);
            }
        }

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        combiner.combine(serverFanouts, hashStore, blobStore, bout, 0, 199l);

        assertEquals(200, bout.size());
    }

//    @Test
    public void test_Finish() throws IOException {
        Combiner combiner = new Combiner();

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        combiner.combine(serverFanouts, hashStore, blobStore, bout, (218870 - 20000), null); // size is 218870

        assertEquals(20000, bout.size());
    }
}
