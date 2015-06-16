package org.hashsplit4j.store;

import org.hashsplit4j.api.BlobStore;
import org.slf4j.LoggerFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.cluster.Node;
import voldemort.cluster.failuredetector.FailureDetectorListener;
import voldemort.versioning.Versioned;

/**
 *
 * @author dylan
 */
public class VoldemortClientBlobStore implements BlobStore {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(VoldemortClientBlobStore.class);
    
    private final StoreClientFactory storeClientFactory;
    private final StoreClient<String, byte[]> client;

    public VoldemortClientBlobStore(StoreClientFactory storeClientFactory, String storeName) {
        this.storeClientFactory = storeClientFactory;
        this.client = this.storeClientFactory.getStoreClient(storeName);
        this.storeClientFactory.getFailureDetector().addFailureDetectorListener(new FailureDetectorListener() {

            @Override
            public void nodeUnavailable(Node node) {
                log.info("Node lost=" + node.getId());
            }

            @Override
            public void nodeAvailable(Node node) {
                log.info("Node gained=" + node.getId());
            }
        });
    }

    @Override
    public void setBlob(String hash, byte[] bytes) {
        log.info("setBlob hash={}", hash);
        if (!hasBlob(hash)) {
            client.put(hash, bytes);
        }
    }

    @Override
    public byte[] getBlob(String hash) {
        log.info("getBlob hash={}", hash);
        Versioned<byte[]> versioned = client.get(hash);
        if (versioned != null) {
            return versioned.getValue();
        }
        return null;
    }

    @Override
    public boolean hasBlob(String hash) {
        log.info("hasBlob hash={}", hash);
        try {
            Versioned<byte[]> versioned = client.get(hash);
            return versioned != null;
        } catch (Exception ex) {
            System.out.println("Error " + ex.getMessage());
            return false;
        }
    }
}
