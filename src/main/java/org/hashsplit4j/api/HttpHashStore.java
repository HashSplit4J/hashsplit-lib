package org.hashsplit4j.api;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpEntity;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;

/**
 * Implements getting and setting fanout hashes over HTTP
 *
 * Can use an optional HashCache to record knowledge of the existence of objects
 * in the remote repository
 *
 * @author brad
 */
public class HttpHashStore implements HashStore {

    private final HttpClient client;
    private final HashCache hashCache;
    private int timeout = 30000;
    private String baseUrl;
    private long gets;
    private long sets;

    /**
     *
     * @param client
     * @param hashCache - optional, may be null. If provided will be used to
     * optimise hasFanout
     */
    public HttpHashStore(HttpClient client, HashCache hashCache) {
        this.client = client;
        this.hashCache = hashCache;
    }

    @Override
    public void setFanout(long hash, List<Long> childCrcs, long actualContentLength) {
        if (hasFanout(hash)) {
            return;
        }
        sets++;
        String s = baseUrl + hash;
        HttpPut p = new HttpPut(s);

        // Copy longs into a byte array
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bout);
        try {
            dos.writeLong(actualContentLength); // send the actualContentLength first
            for (Long l : childCrcs) {
                dos.writeLong(l);
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        byte[] hashes = bout.toByteArray();

        try {
            HttpEntity requestEntity = new ByteArrayEntity(hashes);

            p.setEntity(requestEntity);
            int result = HttpUtils.executeHttpWithStatus(client, p, null);
            if (result < 200 || result >= 300) {
                throw new RuntimeException("Upload failed. result:" + result);
            }
            if (hashCache != null) {
                hashCache.setHash(hash);
            }

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Fanout getFanout(long fanoutHash) {
        gets++;
        String s = baseUrl + fanoutHash;
        HttpGet getMethod = new HttpGet(s);
        int result;
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            result = HttpUtils.executeHttpWithStatus(client, getMethod, bout);
            if (result < 200 || result >= 300) {
                throw new RuntimeException("Download failed. result:" + result + " url: " + s);
            }
            byte[] arr = bout.toByteArray();
            ByteArrayInputStream bin = new ByteArrayInputStream(arr);
            List<Long> list = new ArrayList<Long>();
            DataInputStream din = new DataInputStream(bin);
            long actualContentLength = din.readLong();
            try {
                while (true) {
                    list.add(din.readLong());
                }
            } catch (EOFException e) {
                // cool
            }

            if (hashCache != null) {
                hashCache.setHash(fanoutHash);
            }
            return new FanoutImpl(list, actualContentLength);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

    }

    @Override
    public boolean hasFanout(long fanoutHash) {
        if (hashCache != null) {
            if (hashCache.hasHash(fanoutHash)) { // say that 3 times quickly!!!  :)
                return true;
            }
        }
        String s = baseUrl + fanoutHash;
        HttpOptions opts = new HttpOptions(s);
        int result;
        try {
            result = HttpUtils.executeHttpWithStatus(client, opts, null);
            if (result >= 500) {
                throw new RuntimeException("Server error: " + result);
            }
            if (result == 404) {
                return false;
            }
            if (result >= 200 && result < 300) {
                if (hashCache != null) {
                    hashCache.setHash(fanoutHash);
                }
                return true;
            }
            throw new RuntimeException("Invalid response code: " + result);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    /**
     * Base url to PUT to, hash will be appended. Must end with a slash
     *
     * Eg http://myserver/blobs
     *
     * @return
     */
    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public long getGets() {
        return gets;
    }

    public long getSets() {
        return sets;
    }
}
