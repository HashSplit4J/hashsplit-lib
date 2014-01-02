package org.hashsplit4j.store;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.hashsplit4j.api.BlobStore;

/**
 * BlobStore which accumulates blobs into a byte array
 *
 * @author brad
 */
public class ByteArrayBlobStore implements BlobStore{

    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    
    @Override
    public void setBlob(String l, byte[] bytes) {
        try {
            bout.write(bytes);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public byte[] getBytes() {
        return bout.toByteArray();
    }
    
    @Override
    public byte[] getBlob(String l) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean hasBlob(String l) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
