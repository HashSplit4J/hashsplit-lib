package org.hashsplit4j.store;

import org.hashsplit4j.api.BlobStore;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.versioning.Versioned;

/**
 *
 * @author dylan
 */
public class VoldemortClientBlobStore implements BlobStore {

    private final StoreClientFactory storeClientFactory;
    private final StoreClient<String, byte[]> client;

    public VoldemortClientBlobStore(StoreClientFactory storeClientFactory, String storeName) {
        this.storeClientFactory = storeClientFactory;
        this.client = this.storeClientFactory.getStoreClient(storeName);
    }

    @Override
    public void setBlob(String hash, byte[] bytes) {
        if (!hasBlob(hash)) {
            client.put(hash, bytes);
        }
    }

    @Override
    public byte[] getBlob(String hash) {
        Versioned<byte[]> versioned = client.get(hash);
        if (versioned != null) {
            return versioned.getValue();
        }
        return null;
    }

    @Override
    public boolean hasBlob(String hash) {
        Versioned<byte[]> versioned = client.get(hash);
        return versioned != null;
    }

}
