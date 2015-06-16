package org.hashsplit4j.runnables;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.hashsplit4j.api.BlobImpl;
import org.hashsplit4j.api.BlobStore;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dylan
 */
public class BlobQueueRunnable implements Runnable {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(BlobQueueRunnable.class);

    private final BlobStore blobStore;
    private final BlockingQueue<BlobImpl> queue;

    public BlobQueueRunnable(final BlobStore blobStore, final int queueCapacity) {
        this.blobStore = blobStore;
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
    }

    /**
     * Inserts a blob into this queue if it is possible to do so immediately
     * without violating capacity restrictions, returning true upon success and
     * false if no space is currently available
     *
     * @param hash
     * @param bytes
     * @return
     */
    public boolean addBlob(String hash, byte[] bytes) {
        log.info("Enqueuing blob={}", hash);
        BlobImpl blob = new BlobImpl(hash, bytes);
        return this.queue.offer(blob);
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
