/*
 */
package org.hashsplit4j.store;

import java.io.IOException;
import org.hashsplit4j.api.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author brad
 */
public class SimpleFileDbBlobStore extends AbstractFileDbBlobStore implements BlobStore {

    private static final Logger log = LoggerFactory.getLogger(SimpleFileDbBlobStore.class);

    private final BlobStore wrapped;



    public SimpleFileDbBlobStore(BlobStore wrapped) {
        this.wrapped = wrapped;
    }

    public BlobStore getWrapped() {
        return wrapped;
    }

    public String getBlobKey(String hash) {
        return "b-" + hash;
    }

    @Override
    public void setBlob(String hash, byte[] bytes) {
        wrapped.setBlob(hash, bytes);
    }

    @Override
    public byte[] getBlob(String hash) {
        long startTime = System.currentTimeMillis();
        String key = getBlobKey(hash);
        SimpleFileDb.DbItem item = mapOfItems.get(key);
        if (item != null) {
            try {
                return item.data();
            } catch (IOException ex) {
                log.warn("Exception looking up blob {} from simplefiledb: {}", hash, ex);
            } finally {
                recordHit(startTime);
            }
        }

        startTime = System.currentTimeMillis();
        byte[] bytes = wrapped.getBlob(hash);
        recordMiss(startTime);
        if (enableAdd && bytes != null) {
            // save to the simple DB unless exceeded size
            saveToDb(hash, bytes);
        }
        return bytes;

    }

    @Override
    public boolean hasBlob(String hash) {
        String key = getBlobKey(hash);
        if (mapOfItems.containsKey(key)) {
            return true;
        }
        return wrapped.hasBlob(hash);
    }
}
