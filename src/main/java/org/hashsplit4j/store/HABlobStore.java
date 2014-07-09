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
            curSecondary.setBlob(hash, bytes);
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
            if( arr == null ) {
                if( trySecondaryWhenNotFound && curSecondary != null ) {
                    log.info("Not found in primary, and secondaryInUse is true, so try secondary");
                    arr = curSecondary.getBlob(hash);
                }
            }
            return arr;
        } catch (Exception ex) {
            // try again
            log.warn("Failed to lookup blob, try again once...", ex);
            byte[] arr = curSecondary.getBlob(hash);
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
        if( secondary == null ) {
            // Cant switch
            return ;
        }
        BlobStore newPrimary = curSecondary;
        BlobStore newSecondary = curPrimary;
        
        this.curPrimary = newPrimary;
        this.curSecondary = newSecondary;
    }
}
