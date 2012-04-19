package org.hashsplit4j.api;

import java.util.List;

/**
 *
 * @author brad
 */
public class FanoutImpl implements Fanout{
    private List<Long> hashes;
    private long actualContentLength;

    public FanoutImpl() {
    }

    public FanoutImpl(List<Long> hashes, long actualContentLength) {
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
    public List<Long> getHashes() {
        return hashes;
    }

    public void setHashes(List<Long> hashes) {
        this.hashes = hashes;
    }        
}
