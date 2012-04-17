package org.hashsplit4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.zip.CRC32;

/**
 *
 * @author brad
 */
public class Parser {

    private static final int MASK = 0x0FFF;
    private static final int MEGA_MASK = 0x7FFFF;

    public List<Long> parse(InputStream in, HashStore hashStore) throws IOException {
        Rsum rsum = new Rsum(128);
        int cnt = 0;
        int lastPos = 0;
        int numBlocks = 0;


        List<Long> crcs = new ArrayList<Long>();
        CRC32 crc = new CRC32();
        CRC32 megaCrc = new CRC32();
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        byte[] arr = new byte[1024];
        int s = in.read(arr, 0, 1024);
        List<Long> fanoutCrcs = new ArrayList<Long>();
        while (s >= 0) {
            //System.out.println("got bytes: " + s);
            for (int i = 0; i < s; i++) {
                byte b = arr[i];
                rsum.roll(b);
                crc.update(b);
                megaCrc.update(b);
                bout.write(b);
                int x = rsum.getValue();
                cnt++;
                if ((x & MASK) == MASK) {
                    hashStore.onChunk(crc.getValue(), lastPos, cnt, bout.toByteArray());
                    bout.reset();
                    crcs.add(crc.getValue());
                    crc.reset();
                    if ((x & MEGA_MASK) == MEGA_MASK) {
                        long fanoutCrc = megaCrc.getValue();
                        fanoutCrcs.add(fanoutCrc);
                        hashStore.onFanout(fanoutCrc, crcs);
                        megaCrc.reset();
                        crcs = new ArrayList<Long>();
                    }
                    numBlocks++;
                    lastPos = cnt;
                    rsum.reset();
                }
            }

            s = in.read(arr, 0, 1024);
        }
        // Need to store terminal data
        System.out.println("Store terminal crcs");
        crcs.add(crc.getValue());
        long fanoutCrc = megaCrc.getValue();
        hashStore.onChunk(crc.getValue(), lastPos, cnt, bout.toByteArray());
        hashStore.onFanout(fanoutCrc, crcs);
        fanoutCrcs.add(fanoutCrc);

        System.out.println("num blocks: " + numBlocks);
        System.out.println("total size: " + cnt);
        return fanoutCrcs;
    }
}
