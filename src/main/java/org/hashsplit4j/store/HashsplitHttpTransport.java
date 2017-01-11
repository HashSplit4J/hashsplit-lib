package org.hashsplit4j.store;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.io.IOUtils;
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
 *
 * @author brad
 */
public class HashsplitHttpTransport {

    private final CredentialsProvider credsProvider;
    private int timeout = 5000;
    private final String server;
    private final int port;

    private final HttpHost preemptiveAuthTarget;
    private final AuthCache authCache = new BasicAuthCache();
    private final BasicScheme basicAuth = new BasicScheme();

    public HashsplitHttpTransport(String server, int port, String username, String password) {
        this.server = server;
        this.port = port;
        credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(
                new AuthScope(server, port),
                new UsernamePasswordCredentials(username, password));

        preemptiveAuthTarget = new HttpHost(server, port, "http");
        authCache.put(preemptiveAuthTarget, basicAuth);
    }

    public byte[] get(String path) {
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
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
        long tm = System.currentTimeMillis();
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
        } catch (java.net.SocketTimeoutException ex) {
            tm = System.currentTimeMillis() - tm;
            throw new RuntimeException("Socket timeout: server=" + server + "; port=" + port + "Configured timeout=" + timeout + " actual time=" + tm + "ms", ex);
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

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    @Override
    public String toString() {
        return "HashsplitHttpTransport: " + server + ":" + port;
    }

}
