package org.hashsplit4j;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 *
 * @author brad
 */
public class ByteUtil {
    /**
     * Write the 4 bytes which makeup the long value to the output stream
     * @param l
     * @param out 
     */
    public void writeBytes(long l, OutputStream out) {
        
    }
    
    public long readLong(InputStream in) throws IOException {
        byte[] buf = new byte[4];
        byte b = (byte) in.read();
        
    }
}
