package org.hashsplit4j.store;

import java.util.Arrays;
import java.util.List;
import org.hashsplit4j.api.BlobStore;

/**
 * Supports a common use case of having a more preferential BlobStore (eg local)
 * and a less preferential BlobStore (eg remote)
 *
 * @author brad
 */
public class MultipleBlobStore implements BlobStore {

    private final List<BlobStore> stores;
    private final BlobStore firstStore;

    public MultipleBlobStore(List<BlobStore> stores) {
        this.stores = stores;
        this.firstStore = stores.get(0);
    }

    public MultipleBlobStore(BlobStore... storess) {
        this.stores = Arrays.asList(storess);
        this.firstStore = stores.get(0);
    }

    @Override
    public void setBlob(String hash, byte[] bytes) {
        firstStore.setBlob(hash, bytes);
    }

    @Override
    public boolean hasBlob(String hash) {
        if (hash == null) {
            return false;
        }
        for (BlobStore store : stores) {
            if (store.hasBlob(hash)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public byte[] getBlob(String hash) {
        if (hash == null) {
            return null;
        }

        for (BlobStore store : stores) {
            byte[] arr = store.getBlob(hash);
            if (arr != null) {
                return arr;
            }
        }
        return null;
    }
}
