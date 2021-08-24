package org.hashsplit4j.api;

import java.util.List;

/**
 *
 * @author brad
 */
public class FanoutImpl implements Fanout {

    private List<String> hashes;
    private long actualContentLength;

    public FanoutImpl() {
    }

    public FanoutImpl(List<String> hashes, long actualContentLength) {
        this.hashes = hashes;
        this.actualContentLength = actualContentLength;
    }

    @Override
    public long getActualContentLength() {
        return actualContentLength;
    }

    public void setActualContentLength(long actualContentLength) {
        this.actualContentLength = actualContentLength;
    }

    @Override
    public List<String> getHashes() {
        return hashes;
    }

    public void setHashes(List<String> hashes) {
        this.hashes = hashes;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Fanout: length=").append(actualContentLength);
        if( hashes != null ) {
            sb.append(" hashes: ");
            for( String s : hashes ) {
                sb.append(s).append(",");
            }
        }
        return sb.toString();
    }

}
