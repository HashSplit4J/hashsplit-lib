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
        for( Long fanoutHash : megaCrcs ) {
            List<Long> crcs = hashStore.getFanout(fanoutHash);
            for( Long hash : crcs ) {
                byte[] arr = blobStore.getBlob(hash);
                if( arr == null ) {
                    throw new RuntimeException("Failed to lookup blob: " + hash);
                }
                //System.out.println("Chunk: " + hash + " range: " + lastPos + " - " + (lastPos+arr.length));                
                System.out.println("Write: " + Long.toHexString(hash) + " size: " + arr.length);
                out.write(arr);                
                lastPos+=arr.length;
            }
            //System.out.println("Fanout: " + fanoutHash);
        }
    }
}
