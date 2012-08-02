package org.hashsplit4j.api;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;

/**
 * The parser will take a stream of bytes and split it into chunks with an
 * average size of MASK bytes (eg 8k for BUP, 64k for us). The chunk boundaries are determined by looking at
 * a rolling checksum of the last 128 bytes, when the lowest 13 bits of this
 * checksum we take that as a boundary.
 *
 * This algorithm results in boundaries which are fairly stable with file
 * modifications, so that if a previously chunked file is modified, most of the
 * chunks should still match the new file.
 *
 * The main method to call is parse, and output information is given to the
 * provided HashStore
 *
 * @author brad
 */
public class Parser {

    private static final int MASK = 0xFFF;  // average blob size of 64k
    private static final int FANOUT_MASK = 0x7FFFFFF; // about 1024 hashes per fanout

    public static String parse(File f, BlobStore blobStore, HashStore hashStore) throws FileNotFoundException, IOException {
        Parser parser = new Parser();
        FileInputStream fin = null;
        BufferedInputStream bufIn = null;
        try {
            fin = new FileInputStream(f);            
            bufIn = new BufferedInputStream(fin);
            return parser.parse(bufIn, hashStore, blobStore);
        } finally {
            IOUtils.closeQuietly(bufIn);
            IOUtils.closeQuietly(fin);
        }
    }

    /**
     * Returns a hex enccoded SHA1 hash of the whole file. This can be used to locate the
     * files bytes again
     *
     * @param in
     * @param hashStore
     * @param blobStore
     * @return
     * @throws IOException
     */
    public String parse(InputStream in, HashStore hashStore, BlobStore blobStore) throws IOException {
        Rsum rsum = new Rsum(128);
        int cnt = 0;
        int numBlocks = 0;
        byte[] arr = new byte[1024];
        ByteArrayOutputStream bout = new ByteArrayOutputStream();

        List<String> blobHashes = new ArrayList<String>();
        MessageDigest blobCrc = getCrypt();
        MessageDigest fanoutCrc = getCrypt();
        MessageDigest fileCrc = getCrypt();

        long fanoutLength = 0;
        long fileLength = 0;

        int s = in.read(arr, 0, 1024);
        List<String> fanoutHashes = new ArrayList<String>();
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
//                System.out.println("x=" + x);
//                System.out.println("check mask: " + (x & MASK) + " == " + MASK);
                if ((x & MASK) == MASK) {
                    String blobCrcHex = toHex(blobCrc);
                    //System.out.println("Store blob: " + blobCrcHex);
                    blobStore.setBlob(blobCrcHex, bout.toByteArray());
                    bout.reset();
                    blobHashes.add(blobCrcHex);
                    blobCrc.reset();
                    if ((x & FANOUT_MASK) == FANOUT_MASK) {
                        String fanoutCrcVal = toHex(fanoutCrc);
                        fanoutHashes.add(fanoutCrcVal);
                        System.out.println("fanout: " + fanoutCrcVal);
                        hashStore.setChunkFanout(fanoutCrcVal, blobHashes, fanoutLength);
                        fanoutLength = 0;
                        fanoutCrc.reset();
                        blobHashes = new ArrayList<String>();
                    }
                    numBlocks++;
                    rsum.reset();
                }
            }

            s = in.read(arr, 0, 1024);
        }
        // Need to store terminal data, ie data which has been accumulated since the last boundary
        String blobCrcHex = toHex(blobCrc);
        //System.out.println("Store terminal blob: " + blobCrcHex);
        blobStore.setBlob(blobCrcHex, bout.toByteArray());
        blobHashes.add(blobCrcHex);
        String fanoutCrcVal = toHex(fanoutCrc);        
        hashStore.setChunkFanout(fanoutCrcVal, blobHashes, fanoutLength);
        fanoutHashes.add(fanoutCrcVal);

        // Now store a fanout for the whole file. The contained hashes locate other fanouts
        String fileCrcVal = toHex(fileCrc);
        hashStore.setFileFanout(fileCrcVal, fanoutHashes, fileLength);
        return fileCrcVal;
    }

    public static MessageDigest getCrypt() {
        MessageDigest cript;
        try {
            cript = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);
        }
        return cript;
    }
    
    public static String toHex(MessageDigest crypt) {
        String hash = DigestUtils.shaHex(crypt.digest());
        return hash;
    }
}
