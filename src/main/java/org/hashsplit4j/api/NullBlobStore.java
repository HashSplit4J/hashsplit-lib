package org.hashsplit4j.api;

/**
 *
 * @author brad
 */
public class NullBlobStore implements BlobStore{

    @Override
    public void setBlob(long hash, byte[] bytes) {
        
    }

    @Override
    public byte[] getBlob(long hash) {
        return null;
    }

    @Override
    public boolean hasBlob(long hash) {
        return false;
    }

}
