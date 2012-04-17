package org.hashsplit4j;

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
        long lastPos = 0;
        for( Long fanoutCrc : megaCrcs ) {
            List<Long> crcs = hashStore.getFanout(fanoutCrc);
            for( Long hash : crcs ) {
                byte[] arr = blobStore.getBlob(hash);
                if( arr != null ) {
                    System.out.println("Chunk: " + hash + " range: " + lastPos + " - " + (lastPos+arr.length));                
                } else {
                    throw new RuntimeException("Failed to lookup blob: " + hash);
                }
                out.write(arr);                
                lastPos+=arr.length;
            }
            System.out.println("Fanout: " + fanoutCrc);
        }
    }
}
