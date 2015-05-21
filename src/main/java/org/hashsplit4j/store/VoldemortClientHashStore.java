package org.hashsplit4j.store;

import java.util.List;
import org.hashsplit4j.api.Fanout;
import org.hashsplit4j.api.HashStore;
import org.hashsplit4j.utils.StringFanoutUtils;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.versioning.Versioned;

/**
 *
 * @author dylan
 */
public class VoldemortClientHashStore implements HashStore {

    private final StoreClientFactory storeClientFactory;
    private final StoreClient<String, String> chunkFanoutClient;
    private final StoreClient<String, String> fileFanoutClient;

    public VoldemortClientHashStore(StoreClientFactory storeClientFactory, String fileStoreName, String chunkStoreName) {
        this.storeClientFactory = storeClientFactory;
        this.fileFanoutClient = this.storeClientFactory.getStoreClient(fileStoreName);
        this.chunkFanoutClient = this.storeClientFactory.getStoreClient(chunkStoreName);
    }

    @Override
    public void setChunkFanout(String hash, List<String> blobHashes, long actualContentLength) {
        if (!hasChunk(hash)) {
            String fanout = StringFanoutUtils.formatFanout(blobHashes, actualContentLength);
            chunkFanoutClient.put(hash, fanout);
        }
    }

    @Override
    public void setFileFanout(String hash, List<String> fanoutHashes, long actualContentLength) {
        if (!hasFile(hash)) {
            String fanout = StringFanoutUtils.formatFanout(fanoutHashes, actualContentLength);
            fileFanoutClient.put(hash, fanout);
        }
    }

    @Override
    public Fanout getFileFanout(String fileHash) {
        Versioned<String> versioned = fileFanoutClient.get(fileHash);
        if(versioned != null){
            String f = versioned.getValue();
            Fanout fanout = StringFanoutUtils.parseFanout(f);
            return fanout;
        }
        return null;
    }

    @Override
    public Fanout getChunkFanout(String fanoutHash) {
        Versioned<String> versioned = chunkFanoutClient.get(fanoutHash);
        if(versioned != null){
            String f = versioned.getValue();
            Fanout fanout = StringFanoutUtils.parseFanout(f);
            return fanout;
        }
        return null;
    }

    @Override
    public boolean hasChunk(String fanoutHash) {
        Versioned<String> versioned = chunkFanoutClient.get(fanoutHash);
        return versioned != null;
    }

    @Override
    public boolean hasFile(String fileHash) {
        Versioned<String> versioned = fileFanoutClient.get(fileHash);
        return versioned != null;
    }
}
