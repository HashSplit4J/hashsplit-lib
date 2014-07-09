package org.hashsplit4j.store;

import org.hashsplit4j.api.BlobStore;
import org.slf4j.LoggerFactory;

/**
 * Implements getting and setting blobs over HTTP
 *
 * @author brad
 */
public class HABlobStore implements BlobStore {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(CachingBlobStore.class);

    private final BlobStore primary;
    private final BlobStore secondary;

    private boolean trySecondaryWhenNotFound = true;

    private BlobStore curPrimary;
    private BlobStore curSecondary;

    public HABlobStore(BlobStore primary, BlobStore secondary) {
        this.primary = primary;
        this.secondary = secondary;

        curPrimary = primary;
        curSecondary = secondary;
    }

    @Override
    public void setBlob(String hash, byte[] bytes) {
        try {
            curPrimary.setBlob(hash, bytes);
        } catch (Exception ex) {
            log.warn("setBlob failed on primary: " + curPrimary + " because of: " + ex.getMessage());
            log.warn("try on seconday: " + curSecondary + " ...");            
            curSecondary.setBlob(hash, bytes);
            log.warn("setBlob succeeded on secondary");
            switchStores();
        }
    }

    @Override
    public boolean hasBlob(String hash) {
        byte[] bytes = getBlob(hash);
        return bytes != null;
    }

    @Override
    public byte[] getBlob(String hash) {
        try {
            byte[] arr = curPrimary.getBlob(hash);
            if (arr == null) {
                if (trySecondaryWhenNotFound && curSecondary != null) {
                    log.info("Not found in primary, and secondaryInUse is true, so try secondary");
                    arr = curSecondary.getBlob(hash);
                }
            }
            return arr;
        } catch (Exception ex) {
            log.warn("getBlob failed on primary: " + curPrimary + " because of: " + ex.getMessage());
            log.warn("try on seconday: " + curSecondary + " ...");            
            byte[] arr;
            try {
                arr = curSecondary.getBlob(hash);
                log.warn("getBlob succeeded on secondary");
            } catch (Exception e) {
                throw new RuntimeException("Failed to lookup from secondary: " + secondary, e);
            }
            switchStores();
            return arr;
        }
    }

    public boolean isTrySecondaryWhenNotFound() {
        return trySecondaryWhenNotFound;
    }

    public void setTrySecondaryWhenNotFound(boolean trySecondaryWhenNotFound) {
        this.trySecondaryWhenNotFound = trySecondaryWhenNotFound;
    }

    private synchronized void switchStores() {
        if (secondary == null) {
            log.warn("switchStores: Cant switch because there is no configured secondary");
            // Cant switch
            return;
        }
        log.warn("Switching stores due to primary failure...");
        BlobStore newPrimary = curSecondary;
        BlobStore newSecondary = curPrimary;

        this.curPrimary = newPrimary;
        this.curSecondary = newSecondary;
        log.warn("Done switching stores. New primary=" + curPrimary + " New seconday=" + curSecondary);
    }
}
