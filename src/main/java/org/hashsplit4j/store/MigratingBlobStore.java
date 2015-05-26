package org.hashsplit4j.store;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.hashsplit4j.api.BlobImpl;
import org.hashsplit4j.api.BlobStore;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dylan
 */
public class MigratingBlobStore implements BlobStore {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(MigratingBlobStore.class);

    private final BlobStore newBlobStore;
    private final BlobStore oldBlobStore;
    private final ExecutorService exService;
    private final BlockingQueue<BlobImpl> queue = new ArrayBlockingQueue<>(1000);

    public MigratingBlobStore(BlobStore newBlobStore, BlobStore oldBlobStore) {
        this.newBlobStore = newBlobStore;
        this.oldBlobStore = oldBlobStore;

        exService = Executors.newCachedThreadPool();
        BlobQueueRunnable r = new BlobQueueRunnable(this.newBlobStore);
        exService.submit(r);
    }

    @Override
    public void setBlob(String hash, byte[] bytes) {
        newBlobStore.setBlob(hash, bytes);
    }

    @Override
    public byte[] getBlob(String hash) {
        log.info("getBlob={}", hash);
        if (newBlobStore.hasBlob(hash)) {
            log.info("got blob from={}", newBlobStore);
            return newBlobStore.getBlob(hash);
        } else if (oldBlobStore.hasBlob(hash)) {
            log.info("got blob from={}", oldBlobStore);
            byte[] data = oldBlobStore.getBlob(hash);
            enqueue(hash, data);
            return data;
        }
        return null;
    }

    @Override
    public boolean hasBlob(String hash) {
        return newBlobStore.hasBlob(hash) || oldBlobStore.hasBlob(hash);
    }

    private void enqueue(String hash, byte[] bytes) {
        log.info("Enqueuing blob={}", hash);
        BlobImpl blob = new BlobImpl(hash, bytes);
        queue.offer(blob);
    }

    public class BlobQueueRunnable implements Runnable {

        private final BlobStore blobStore;

        public BlobQueueRunnable(BlobStore blobStore) {
            this.blobStore = blobStore;
        }

        @Override
        public void run() {
            BlobImpl blob;
            while (true) {
                try {
                    blob = queue.take();
                    if (blob != null) {
                        blobStore.setBlob(blob.getHash(), blob.getBytes());
                    }
                } catch (Exception ex) {
                    if (ex instanceof InterruptedException) {
                        log.error("An InterruptedException was thrown with queue {}", queue, ex);
                        throw new RuntimeException(ex);
                    } else {
                        log.error("Exception inserting blob into store:{}", blobStore, ex);
                    }
                }
            }
        }
    }
}
