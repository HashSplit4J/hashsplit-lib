package org.hashsplit4j;

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
    public void setBlob(long hash, int offset, byte[] bytes) {
        byte[] arr = remoteBlobStore.getBlob(hash);
        if( arr == null ) {
            //System.out.println("Remote does not have blob: " + hash);
            remoteBlobStore.setBlob(hash, offset, bytes);
        } else {
            //System.out.println("Found existing blob: " + hash);
        }    
    }

    @Override
    public byte[] getBlob(Long hash) {
        return remoteBlobStore.getBlob(hash);
    }

}
