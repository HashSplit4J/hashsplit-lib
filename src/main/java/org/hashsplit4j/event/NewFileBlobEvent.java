/*
 */

package org.hashsplit4j.event;

import java.io.File;

/**
 * Fired when a new blob has been added to the local store
 *
 * @author brad
 */
public class NewFileBlobEvent extends NewBlobEvent {

    private final File file;
    private final File root;
    
    public NewFileBlobEvent(String hash, File file, File root, byte[] data) {
        super(hash, data);
        this.file = file;
        this.root = root;
    }

    public File getFile() {
        return file;
    }

    public File getRoot() {
        return root;
    }

    @Override
    public String toString() {
        if( file != null ) {
            return "NewBlob: " + file.getAbsolutePath();
        } else {
            return "NewBlob: none";
        }
    }
        
    
    
}
