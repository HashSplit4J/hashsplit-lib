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
        try {
            newBlobStore.setBlob(hash, bytes);
            return;
        } catch (Exception ex) {
            log.warn("Failed to store blob to newBlobStore {} with message {}", newBlobStore, ex.getMessage(), ex);
        }

        try {
            log.info("Trying to store blob on oldBlobStore {}", oldBlobStore);
            oldBlobStore.setBlob(hash, bytes);
        } catch (Exception ex) {
            log.warn("Failed to store blob to oldBlobStore {} with message {}", oldBlobStore, ex.getMessage(), ex);
        }
    }

    @Override
    public byte[] getBlob(String hash) {
        log.info("getBlob={}", hash);

        try {
            if (newBlobStore.hasBlob(hash)) {
                log.info("got blob from={}", newBlobStore);
                return newBlobStore.getBlob(hash);
            }
        } catch (Exception ex) {
            log.warn("getBlob Failed on newBlobStore {} because of:{}", newBlobStore, ex.getMessage(), ex);
        }

        try {
            if (oldBlobStore.hasBlob(hash)) {
                log.info("got blob from={}", oldBlobStore);
                byte[] data = oldBlobStore.getBlob(hash);
                enqueue(hash, data);
                return data;
            }
        } catch (Exception ex) {
            log.warn("getBlob Failed on oldBlobStore {} because of:{}", oldBlobStore, ex.getMessage(), ex);
        }

        return null;
    }

    @Override
    public boolean hasBlob(String hash) {
        try {
            return newBlobStore.hasBlob(hash) || oldBlobStore.hasBlob(hash);
        } catch (Exception ex) {
            log.warn("failed hasBlob with message {}", ex.getMessage(), ex);
        }
        return false;
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
