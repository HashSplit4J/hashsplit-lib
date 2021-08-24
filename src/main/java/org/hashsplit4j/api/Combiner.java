package org.hashsplit4j.api;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Puts files back together
 *
 * @author brad
 */
public class Combiner {

    private static final Logger log = LoggerFactory.getLogger(Combiner.class);

    private long currentByte = 0;
    private int currentFanout = 0;
    private int currentBlob = 0;
    private int currentBlobByte;
    private long bytesWritten;
    private boolean canceled;

    public void combine(List<String> fanoutHashes, HashStore hashStore, BlobStore blobStore, OutputStream out) throws IOException {
        if (canceled) {
            throw new IOException("Operation cancelled");
        }
        for (String fanoutHash : fanoutHashes) {
            Fanout fanout = hashStore.getChunkFanout(fanoutHash);
            if (fanout != null) {
                List<String> hashes = fanout.getHashes();
                if (hashes != null) {
                    for (String hash : fanout.getHashes()) {
                        if (canceled) {
                            throw new IOException("Operation cancelled");
                        }

                        byte[] arr = blobStore.getBlob(hash);
                        if (arr == null) {
                            throw new RuntimeException("Failed to lookup blob: " + hash + ", from chunk fanout " + fanoutHash + ", using blobstore " + blobStore + ", hashstore=" + hashStore);
                        }
                        out.write(arr);
                        bytesWritten += arr.length;
                    }
                } else {
                    log.warn("Got null hashes for fanout: " + fanoutHash);
                }
            } else {
                log.warn("Did not find fanout: " + fanoutHash);
                //throw new RuntimeException("Did not find chunk fanout: " + fanoutHash);
            }
        }

    }

    public void combine(List<String> megaCrcs, HashStore hashStore, BlobStore blobStore, OutputStream out, long start, Long finish) throws IOException {
        seek(start, megaCrcs, hashStore, blobStore);
        writeToFinish(finish, megaCrcs, hashStore, blobStore, out);
    }

    private void seek(long start, List<String> megaCrcs, HashStore hashStore, BlobStore blobStore) throws IOException {
        while (currentFanout < megaCrcs.size()) {
            String fanoutHash = megaCrcs.get(currentFanout);
            Fanout fanout = hashStore.getChunkFanout(fanoutHash);
            long fanoutEnd = currentByte + fanout.getActualContentLength();
            if (fanoutEnd >= start) {
                while (currentBlob < fanout.getHashes().size()) {
                    if (canceled) {
                        throw new IOException("Operation cancelled");
                    }

                    String blobHash = fanout.getHashes().get(currentBlob);
                    byte[] arr = blobStore.getBlob(blobHash);
                    if (arr == null) {
                        throw new RuntimeException("Failed to find blob in fanout. Blob hash: " + blobHash);
                    }
                    if (currentByte + arr.length >= start) { // if end is after beginning of range, then this is the blob we want
                        currentBlobByte = (int) (start - currentByte);
                        currentByte += currentBlobByte;
                        return;
                    } else {
                        currentByte += arr.length;
                    }
                    currentBlob++;
                }
            } else {
                currentByte += fanout.getActualContentLength();
            }
            currentFanout++;
        }
    }

    private void writeToFinish(Long finish, List<String> megaCrcs, HashStore hashStore, BlobStore blobStore, OutputStream out) throws IOException {
        while (currentFanout < megaCrcs.size() && (finish == null || currentByte < finish)) {
            String fanoutHash = megaCrcs.get(currentFanout);
            Fanout fanout = hashStore.getChunkFanout(fanoutHash);
            while (currentBlob < fanout.getHashes().size() && (finish == null || currentByte < finish)) {
                if (canceled) {
                    throw new IOException("Operation cancelled");
                }

                String hash = fanout.getHashes().get(currentBlob);
                byte[] arr = blobStore.getBlob(hash);
                if (arr == null) {
                    throw new RuntimeException("Couldnt locate blob: " + hash);
                }

                int numBytes;
                if (finish == null) {
                    numBytes = arr.length - currentBlobByte;
                } else {
                    long bytesLeftToWrite = finish - currentByte + 1;
                    long blobBytesLeft = arr.length - currentBlobByte;
                    if (blobBytesLeft <= bytesLeftToWrite) {
                        // write all remaining bytes from this blob
                        numBytes = arr.length - currentBlobByte;
                    } else {
                        // less bytes to write then available, so write up to bytesLeftToWrite
                        numBytes = (int) bytesLeftToWrite;
                    }
                }

//                if (finish == null || currentByte + arr.length < finish) {
//                    // write all remaining bytes from this blob
//                    System.out.println("write all remaining");
//                    numBytes = arr.length - currentBlobByte;
//                } else {
//                    System.out.println("write up to finish");
//                    numBytes = (int) (finish - currentByte + 1);
//                }
                try {
                    out.write(arr, currentBlobByte, numBytes);
                } catch (Throwable e) {
                    log.error("Exception writing bytes: finish={} currentByte={} ", finish, currentByte);
                    throw new RuntimeException("Failed to write bytes: currentBlobByte=" + currentBlobByte + " numBytes=" + numBytes + " array size=" + arr.length, e);
                }
                bytesWritten += numBytes;
                currentBlobByte = 0;
                currentByte += numBytes;
                currentBlob++;
            }
            currentFanout++;
            currentBlob = 0;
        }
    }

    public long getBytesWritten() {
        return bytesWritten;
    }

    public int getCurrentBlob() {
        return currentBlob;
    }

    public int getCurrentBlobByte() {
        return currentBlobByte;
    }

    public long getCurrentByte() {
        return currentByte;
    }

    public int getCurrentFanout() {
        return currentFanout;
    }

    public boolean isCanceled() {
        return canceled;
    }

    public void setCanceled(boolean canceled) {
        this.canceled = canceled;
    }
}
