package org.hashsplit4j.api;

import java.util.List;

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

    @Override
    public void setBlob(String hash, byte[] bytes) {
    }

    @Override
    public boolean hasBlob(String hash) {
        for (BlobStore store : stores) {
            if (store.hasBlob(hash)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public byte[] getBlob(String hash) {
        for (BlobStore store : stores) {
            byte[] arr = store.getBlob(hash);
            if( arr != null ) {
                return arr;
            }
        }
        return null;
    }
}
