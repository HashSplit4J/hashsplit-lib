package org.hashsplit4j.runnables;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.hashsplit4j.api.BlobImpl;
import org.hashsplit4j.store.SimpleFileDb;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dylan
 */
public class SimpleFileDbQueueRunnable implements Runnable {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(SimpleFileDbQueueRunnable.class);
    public static final int MAX_ERRORS = 100;   // if too many errors then dont try to save any more

    private final SimpleFileDb db;
    private final BlockingQueue<BlobImpl> queue;
    private final long maxFileSize;

    private int errors;

    public SimpleFileDbQueueRunnable(final SimpleFileDb db, final int queueCapacity, long maxFileSize) {
        this.db = db;
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.maxFileSize = maxFileSize;
    }

    public long getMaxFileSize() {
        return maxFileSize;
    }

    public long getFileSize() {
        return db.getValuesFileSize();
    }

    public int getQueueSize() {
        return queue.size();
    }

    public int getErrors() {
        return errors;
    }

    /**
     * Inserts a blob into this queue if it is possible to do so immediately
     * without violating capacity restrictions, returning true upon success and
     * false if for any reason the blob is not enqueued (eg no space is
     * currently available, or already enqueued)
     *
     * @param hash
     * @param bytes
     * @return true upon success and false if no space is currently available
     */
    public boolean add(String hash, byte[] bytes) {
        if (errors > MAX_ERRORS) {
            log.warn("addBlob: Too many errors, will not try to save to MCS");
            return false;
        } else {
            if (db.getValuesFileSize() < maxFileSize) {
                log.info("Enqueuing blob={} size={}", hash, bytes.length);
                BlobImpl blob = new BlobImpl(hash, bytes);
                return this.queue.offer(blob);
            } else {
                log.info("Cache file has exceeded max size, will not add to cache {}", hash);
                return false;
            }
        }
    }

    @Override
    public void run() {
        BlobImpl blob = null;
        while (true) {
            try {
                blob = queue.take();
                if (blob != null) {
                    db.put(blob.getHash(), blob.getBytes());
                }
            } catch (Exception ex) {
                errors++;
                if (ex instanceof InterruptedException) {
                    log.error("An InterruptedException was thrown with queue {}", queue, ex);
                    throw new RuntimeException(ex);
                } else {
                    log.error("Exception inserting blob into DB: Msg: {}", ex.getMessage(), ex);
                }
            }
        }
    }

}
