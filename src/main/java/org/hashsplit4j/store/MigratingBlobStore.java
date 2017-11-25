package org.hashsplit4j.store;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.hashsplit4j.api.BlobStore;
import org.hashsplit4j.runnables.BlobQueueRunnable;
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
    private final BlobQueueRunnable queue;

    public MigratingBlobStore(BlobStore newBlobStore, BlobStore oldBlobStore) {
        this.newBlobStore = newBlobStore;
        this.oldBlobStore = oldBlobStore;
        this.queue = new BlobQueueRunnable(this.newBlobStore, 1000);
        exService = Executors.newCachedThreadPool();
        exService.submit(this.queue);
    }

    @Override
    public void setBlob(String hash, byte[] bytes) {
        try {
            newBlobStore.setBlob(hash, bytes);
            return;
        } catch (Exception ex) {
            log.warn("Failed to store blob to newBlobStore {} with message {}", newBlobStore, ex.getMessage(), ex);
            enqueue(hash, bytes);
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
        log.trace("getBlob={}", hash);

        try {
            if (newBlobStore.hasBlob(hash)) {
                log.trace("got blob from={}", newBlobStore);
                return newBlobStore.getBlob(hash);
            } else {
                log.info("Could not find blob {} on newBlobStore {}", hash, newBlobStore);
            }
        } catch (Exception ex) {
            log.warn("getBlob Failed on newBlobStore {} because of:{}", newBlobStore, ex.getMessage(), ex);
        }

        try {
            if (oldBlobStore.hasBlob(hash)) {
                log.trace("got blob from={}", oldBlobStore);
                byte[] data = oldBlobStore.getBlob(hash);
                enqueue(hash, data);
                return data;
            } else {
                log.info("Could not find blob {} on oldBlobStore {}", hash, oldBlobStore);
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
        log.trace("Enqueuing blob={}", hash);
        queue.addBlob(hash, bytes);
    }
}
