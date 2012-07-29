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

    long currentByte = 0;
    int currentFanout = 0;
    int currentBlob = 0;
    int currentBlobByte;

    public void combine(List<Long> megaCrcs, HashStore hashStore, BlobStore blobStore, OutputStream out) throws IOException {
        for (Long fanoutHash : megaCrcs) {
            Fanout fanout = hashStore.getFanout(fanoutHash);
            for (Long hash : fanout.getHashes()) {
                byte[] arr = blobStore.getBlob(hash);
                if (arr == null) {
                    throw new RuntimeException("Failed to lookup blob: " + hash);
                }
                out.write(arr);
            }
        }

    }

    public void combine(List<Long> megaCrcs, HashStore hashStore, BlobStore blobStore, OutputStream out, long start, Long finish) throws IOException {
        seek(start, megaCrcs, hashStore, blobStore);
        writeToFinish(finish, megaCrcs, hashStore, blobStore, out);
    }

    private void seek(long start, List<Long> megaCrcs, HashStore hashStore, BlobStore blobStore) {
        while (currentFanout < megaCrcs.size()) {
            long fanoutHash = megaCrcs.get(currentFanout);
            Fanout fanout = hashStore.getFanout(fanoutHash);
            long fanoutEnd = currentByte + fanout.getActualContentLength();
            if (fanoutEnd >= start) {
                while (currentBlob < fanout.getHashes().size()) {
                    long blobHash = fanout.getHashes().get(currentBlob);
                    byte[] arr = blobStore.getBlob(blobHash);
                    if( currentByte + arr.length >= start) { // if end is after beginning of range, then this is the blob we want
                        currentBlobByte = (int) (start - currentByte);
                        currentByte += currentBlobByte;
                        return ;
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

//    private void writeAll(List<Long> megaCrcs, HashStore hashStore, BlobStore blobStore, OutputStream out) throws IOException {
//        while (currentFanout < megaCrcs.size()) {
//            Long fanoutHash = megaCrcs.get(currentFanout);
//            Fanout fanout = hashStore.getFanout(fanoutHash);
//            while (currentBlob < fanout.getHashes().size()) {
//                long hash = fanout.getHashes().get(currentBlob);
//                byte[] arr = blobStore.getBlob(hash);
//                if (currentBlobByte > 0) { // we might be part way through a blob
//                    while (currentBlobByte < arr.length) {
//                        byte b = arr[currentBlobByte];
//                        out.write(b);
//                    }
//                } else {
//                    out.write(arr);
//                }
//                currentBlobByte = 0;
//            }
//            currentBlob = 0;
//        }
//    }

    private void writeToFinish(Long finish, List<Long> megaCrcs, HashStore hashStore, BlobStore blobStore, OutputStream out) throws IOException {
        while (currentFanout < megaCrcs.size() && (finish == null || currentByte < finish )) {
            Long fanoutHash = megaCrcs.get(currentFanout);
            Fanout fanout = hashStore.getFanout(fanoutHash);
            while (currentBlob < fanout.getHashes().size() && (finish == null || currentByte < finish )) {
                long hash = fanout.getHashes().get(currentBlob);
                byte[] arr = blobStore.getBlob(hash);
                int numBytes;
                if( finish == null || currentByte + arr.length < finish) {
                   // write all remaining bytes
                    numBytes = arr.length - currentBlobByte;
                } else {
                    numBytes = (int) (finish - currentByte + 1);
                }
                out.write(arr, currentBlobByte, numBytes);
                currentBlobByte = 0;
                currentByte+=numBytes;
                currentBlob++;
            }
            currentFanout++;
            currentBlob = 0;
        }
    }
}
