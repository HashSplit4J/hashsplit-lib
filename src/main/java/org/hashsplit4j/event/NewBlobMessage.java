package org.hashsplit4j.event;

import java.io.Serializable;

/**
 * Sent to the cluster to inform of a new blob
 *
 * @author brad
 */
public class NewBlobMessage implements Serializable {
    private final String hash;
    private final byte[] data;

    public NewBlobMessage(String hash, byte[] data) {
        this.hash = hash;
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }

    public String getHash() {
        return hash;
    }
    
    
}
