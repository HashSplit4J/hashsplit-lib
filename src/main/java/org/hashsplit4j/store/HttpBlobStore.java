package org.hashsplit4j.store;

import io.milton.common.Path;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.io.IOUtils;
import org.hashsplit4j.api.BlobStore;
import org.slf4j.LoggerFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

/**
 * Implements getting and setting blobs over HTTP
 *
 * @author brad
 */
public class HttpBlobStore implements BlobStore {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(CachingBlobStore.class);

    private final CredentialsProvider credsProvider;
    private int timeout = 2000;
    private final String server;
    private final int port;
    private Path basePath;
    private long gets;
    private long sets;


    private final HttpHost preemptiveAuthTarget;
    private final AuthCache authCache = new BasicAuthCache();
    private final BasicScheme basicAuth = new BasicScheme();

    public HttpBlobStore(String server, int port, String rootPath, String username, String password) {
        this.server = server;
        this.port = port;
        this.basePath = Path.path(rootPath);
        credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(
                new AuthScope(server, port),
                new UsernamePasswordCredentials(username, password));

        preemptiveAuthTarget = new HttpHost(server, port, "http");
        authCache.put(preemptiveAuthTarget, basicAuth);
    }

    @Override
    public void setBlob(String hash, byte[] bytes) {
        if (hasBlob(hash)) {
            return;
        }
        Path destPath = basePath.child(hash + "");
        put(destPath.toString(), bytes);        
    }

    @Override
    public boolean hasBlob(String hash) {
        byte[] bytes = getBlob(hash);
        return bytes != null;
    }

    @Override
    public byte[] getBlob(String hash) {
        Path destPath = basePath.child(hash + "");
        return get(destPath.toString());
    }

    private byte[] getBlobSingle(String hash) throws Exception {
        Path destPath = basePath.child(hash + "");
        return get(destPath.toString());
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
     * Eg http://myserver/blobs/
     *
     * @return
     */
    public String getBaseUrl() {
        return basePath.toString();
    }

    public void setBaseUrl(String baseUrl) {
        this.basePath = Path.path(baseUrl);
    }

    public long getGets() {
        return gets;
    }

    public long getSets() {
        return sets;
    }



    

    public byte[] get(String path) {
        HttpClientContext localContext = HttpClientContext.create();
        localContext.setAuthCache(authCache);
        
        RequestConfig reqConfig = RequestConfig.custom()
                .setSocketTimeout(timeout)
                .setConnectTimeout(timeout)
                .build();
        CloseableHttpClient client = HttpClients.custom()
                .setDefaultCredentialsProvider(credsProvider)
                .setDefaultRequestConfig(reqConfig)
                .build();
        try {
            URI uri = new URI("http", null, server, port, path, null, null);
            HttpGet m = new HttpGet(uri);
            ResponseHandler<byte[]> responseHandler = new ResponseHandler<byte[]>() {

                @Override
                public byte[] handleResponse(final HttpResponse response) throws ClientProtocolException, IOException {
                    int status = response.getStatusLine().getStatusCode();
                    if (status >= 200 && status < 300) {
                        HttpEntity entity = response.getEntity();
                        return entity != null ? EntityUtils.toByteArray(entity) : new byte[0];
                    } else if (status == 404) {
                        return null;
                    } else {
                        throw new ClientProtocolException("Unexpected response status: " + status);
                    }
                }

            };
            byte[] responseBody = client.execute(m, responseHandler, localContext);
            return responseBody;

        } catch (URISyntaxException | IOException ex) {
            throw new RuntimeException("server=" + server + "; port=" + port + "; path=" + path, ex);
        } finally {
            IOUtils.closeQuietly(client);
        }
    }

    public void put(String path, byte[] bytes) {
        HttpClientContext localContext = HttpClientContext.create();
        localContext.setAuthCache(authCache);

        RequestConfig reqConfig = RequestConfig.custom()
                .setSocketTimeout(timeout)
                .setConnectTimeout(timeout)
                .build();
        CloseableHttpClient client = HttpClients.custom()
                .setDefaultCredentialsProvider(credsProvider)
                .setDefaultRequestConfig(reqConfig)
                .build();
        CloseableHttpResponse response = null;
        try {
            URI uri = new URI("http", null, server, port, path, null, null);
            HttpPut m = new HttpPut(uri);
            HttpEntity requestEntity = new ByteArrayEntity(bytes);
            m.setEntity(requestEntity);

            response = client.execute(m, localContext);
            int status = response.getStatusLine().getStatusCode();
            if (status >= 200 && status < 300) {
                // all good
            } else {
                throw new RuntimeException("Unexpected response status: " + status);
            }

        } catch (URISyntaxException | IOException ex) {
            throw new RuntimeException("server=" + server + "; port=" + port + "; path=" + path, ex);
        } finally {
            IOUtils.closeQuietly(response);
            IOUtils.closeQuietly(client);
        }
    }
}
