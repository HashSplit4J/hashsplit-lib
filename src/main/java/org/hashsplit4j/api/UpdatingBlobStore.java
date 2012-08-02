package org.hashsplit4j.api;

/**
 *
 * @author brad
 */
public class UpdatingBlobStore implements BlobStore{

    private final BlobStore remoteBlobStore;

    public UpdatingBlobStore(BlobStore remoteBlobStore) {
        this.remoteBlobStore = remoteBlobStore;
    }
    
    
    
    @Override
    public void setBlob(String hash, byte[] bytes) {
        if( !remoteBlobStore.hasBlob(hash) ) {
            //System.out.println("Remote does not have blob: " + hash);
            remoteBlobStore.setBlob(hash, bytes);
        } else {
            //System.out.println("Found existing blob: " + hash);
        }    
    }

    @Override
    public byte[] getBlob(String hash) {
        return remoteBlobStore.getBlob(hash);
    }

    @Override
    public boolean hasBlob(String hash) {
        return remoteBlobStore.hasBlob(hash);
    }
    
    

}
