package org.hashsplit4j.api;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

/**
 * Puts files back together
 *
 * @author brad
 */
public class Combiner {
    public void combine(List<Long> megaCrcs, HashStore hashStore, BlobStore blobStore, OutputStream out) throws IOException {
        for( Long fanoutHash : megaCrcs ) {
            Fanout fanout = hashStore.getFanout(fanoutHash);
            for( Long hash : fanout.getHashes() ) {
                byte[] arr = blobStore.getBlob(hash);
                if( arr == null ) {
                    throw new RuntimeException("Failed to lookup blob: " + hash);
                }
                out.write(arr);                
            }
        }
    }
}
