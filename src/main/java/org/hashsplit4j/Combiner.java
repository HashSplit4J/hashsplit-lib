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
    public void combine(List<Long> megaCrcs, HashStore hashStore, OutputStream out) throws IOException {
        long lastPos = 0;
        for( Long fanoutCrc : megaCrcs ) {
            List<Long> crcs = hashStore.getCrcsFromFanout(fanoutCrc);
            for( Long crc : crcs ) {
                byte[] arr = hashStore.getBlob(crc);
                System.out.println("Chunk: " + crc + " range: " + lastPos + " - " + (lastPos+arr.length));                
                out.write(arr);                
                lastPos+=arr.length;
            }
            System.out.println("Fanout: " + fanoutCrc);
        }
    }
}
