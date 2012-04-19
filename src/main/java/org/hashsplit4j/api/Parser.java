package org.hashsplit4j.api;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * The parser will take a stream of bytes and split it into chunks with
 * an average size of 8192 bytes. The chunk boundaries are determined by
 * looking at a rolling checksum of the last 128 bytes, when the lowest 13 bits
 * of this checksum we take that as a boundary.
 * 
 * This algorithm results in boundaries which are fairly stable with file modifications,
 * so that if a previously chunked file is modified, most of the chunks should still
 * match the new file.
 * 
 * The main method to call is parse, and output information is given to the provided
 * HashStore
 *
 * @author brad
 */
public class Parser {

    private static final int MASK = 0x0FFF;
    private static final int FANOUT_MASK = 0x7FFFF;

    /**
     * Returns a hash of the whole file. This can be used to locate the 
     * 
     * @param in
     * @param hashStore
     * @param blobStore
     * @return
     * @throws IOException 
     */
    public long parse(InputStream in, HashStore hashStore, BlobStore blobStore) throws IOException {
        Rsum rsum = new Rsum(128);
        int cnt = 0;
        int numBlocks = 0;
        byte[] arr = new byte[1024];
        ByteArrayOutputStream bout = new ByteArrayOutputStream();        

        List<Long> crcs = new ArrayList<Long>();
        CRC32 blobCrc = new CRC32();
        CRC32 fanoutCrc = new CRC32();
        CRC32 fileCrc = new CRC32();
        
        long fanoutLength = 0;
        long fileLength = 0;        
        
        int s = in.read(arr, 0, 1024);
        List<Long> fanoutCrcs = new ArrayList<Long>();
        while (s >= 0) {
            for (int i = 0; i < s; i++) {
                byte b = arr[i];
                rsum.roll(b);
                blobCrc.update(b);
                fanoutCrc.update(b);
                fileCrc.update(b);
                fanoutLength++;
                fileLength++;
                bout.write(b);
                int x = rsum.getValue();
                cnt++;
                if ((x & MASK) == MASK) {
                    blobStore.setBlob(blobCrc.getValue(), bout.toByteArray());
                    bout.reset();
                    crcs.add(blobCrc.getValue());
                    blobCrc.reset();
                    if ((x & FANOUT_MASK) == FANOUT_MASK) {
                        long fanoutCrcVal = fanoutCrc.getValue();
                        fanoutCrcs.add(fanoutCrcVal);
                        hashStore.setFanout(fanoutCrcVal, crcs, fanoutLength);
                        fanoutLength = 0;
                        fanoutCrc.reset();
                        crcs = new ArrayList<Long>();
                    }
                    numBlocks++;
                    rsum.reset();
                }
            }

            s = in.read(arr, 0, 1024);
        }
        // Need to store terminal data, ie data which has been accumulated since the last boundary
        crcs.add(blobCrc.getValue());
        long fanoutCrcVal = fanoutCrc.getValue();
        blobStore.setBlob(blobCrc.getValue(), bout.toByteArray());
        hashStore.setFanout(fanoutCrcVal, crcs, fanoutLength);
        fanoutCrcs.add(fanoutCrcVal);
        
        // Now store a fanout for the whole file. The contained hashes locate other fanouts
        long fileCrcVal = fileCrc.getValue();                
        hashStore.setFanout(fileCrcVal, fanoutCrcs, fileLength);
                
        return fileCrcVal;
    }
}
