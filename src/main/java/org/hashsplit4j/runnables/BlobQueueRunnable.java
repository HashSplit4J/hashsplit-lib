package org.hashsplit4j.runnables;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import java.nio.charset.Charset;
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
    public static final int MAX_ERRORS = 100;   // if too many errors then dont try to save any more

    private final BlobStore blobStore;
    private final BlockingQueue<BlobImpl> queue;
    private final boolean enableRetries;
    private final Funnel<CharSequence> funnel = Funnels.stringFunnel(Charset.defaultCharset());
    private BloomFilter<CharSequence> f1;

    private int errors;

    public BlobQueueRunnable(final BlobStore blobStore, final int queueCapacity) {
        this.blobStore = blobStore;
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.enableRetries = true;
        f1 = BloomFilter.create(funnel, 20000, 0.01d);
    }

    public BlobQueueRunnable(final BlobStore blobStore, final int queueCapacity, boolean enableRetries) {
        this.blobStore = blobStore;
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.enableRetries = enableRetries;
        f1 = BloomFilter.create(funnel, 20000, 0.01d);
    }

    public int getQueueSize() {
        return queue.size();
    }

    public int getErrors() {
        return errors;
    }

    public boolean isEnableRetries() {
        return enableRetries;
    }

    public BlobStore getBlobStore() {
        return blobStore;
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
    public boolean addBlob(String hash, byte[] bytes) {
        if (errors > MAX_ERRORS) {
            log.warn("addBlob: Too many errors, will not try to save to MCS");
            return false;
        } else {
            if (f1.mightContain(hash)) {
                log.info("addBlob: already probably enqueued, hash={}", hash);
                return false;
            }
            f1.put(hash);
            log.info("Enqueuing blob={} size={}", hash, bytes.length);
            BlobImpl blob = new BlobImpl(hash, bytes);
            return this.queue.offer(blob);
        }
    }

    @Override
    public void run() {
        BlobImpl blob = null;
        while (true) {
            try {
                blob = queue.take();
                if (blob != null) {
                    blobStore.setBlob(blob.getHash(), blob.getBytes());
                }
            } catch (Exception ex) {
                errors++;
                if (ex instanceof InterruptedException) {
                    log.error("An InterruptedException was thrown with queue {}", queue, ex);
                    throw new RuntimeException(ex);
                } else {
                    log.error("Exception inserting blob into store:{} | Msg: {}", blobStore, ex.getMessage(), ex);
                    if (enableRetries) {
                        if (blob != null) {
                            if (errors < MAX_ERRORS) {
                                log.info("Retries are enabled, so re-submit blob. Error count={}, Max errors={}", errors, MAX_ERRORS);
                                queue.offer(blob);
                            }
                        }
                    }
                }
            }
        }
    }

}
