package org.hashsplit4j.api;

/**
 * Supports a common use case of having a more preferential BlobStore (eg local) and a 
 * less preferential BlobStore (eg remote)
 *
 * @author brad
 */
public class MultipleBlobStore implements BlobStore{
    private final BlobStore store1;
    private final BlobStore store2;

    private int store1Hits;
    private int store2Hits;
    
    public MultipleBlobStore(BlobStore store1, BlobStore store2) {
        this.store1 = store1;
        this.store2 = store2;
    }
       

    @Override
    public void setBlob(long hash, int offset, byte[] bytes) {

    }

    @Override
    public boolean hasBlob(long hash) {
        if( store1.hasBlob(hash)) {
            return true;
        }
        return store2.hasBlob(hash);
    }

    
    
    @Override
    public byte[] getBlob(long hash) {
        byte[] arr = store1.getBlob(hash);
        if( arr == null ) {
            arr = store2.getBlob(hash);
            if( arr != null ) {
                store2Hits++;
            }
        } else {
            store1Hits++;
        }
        return arr;
    }

    public int getStore1Hits() {
        return store1Hits;
    }

    public int getStore2Hits() {
        return store2Hits;
    }
    
    
}
