/*
 */

package org.hashsplit4j.event;

import io.milton.event.Event;

/**
 *
 * @author brad
 */
public class NewBlobEvent implements Event{
    private final String hash;
    private final byte[] data;    

    public NewBlobEvent(String hash, byte[] data) {
        this.hash = hash;
        this.data = data;
    }



    public String getHash() {
        return hash;
    }
    
    public byte[] getData() {
        return data;
    }   
            
}
