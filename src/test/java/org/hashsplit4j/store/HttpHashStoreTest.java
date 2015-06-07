/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hashsplit4j.store;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.easymock.EasyMockRule;
import org.easymock.Mock;
import org.hashsplit4j.api.Fanout;
import org.junit.Test;
import org.junit.Rule;
import org.easymock.*;
import static org.easymock.EasyMock.*;
import org.hashsplit4j.api.FanoutSerializationUtils;
import org.junit.Assert;
import static org.junit.Assert.assertNotNull;

/**
 *
 * @author brad
 */
public class HttpHashStoreTest extends EasyMockSupport {

    @Rule
    public EasyMockRule rule = new EasyMockRule(this);

    @Mock
    HashsplitHttpTransport httpTransport;

    HttpHashStore hashStore;

    byte[] fanoutBytes;

    public HttpHashStoreTest() throws IOException {
        // Just build a test fanout to work with
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        List<String> hashes = new ArrayList<>();
        hashes.add("A1");
        hashes.add("A2");
        hashes.add("A3");
        FanoutSerializationUtils.writeFanout(hashes, 1000, bout);

        fanoutBytes = bout.toByteArray();

        //System.out.println("fanout: " + new String(fanoutBytes));
    }

    @Test
    public void testGetFileFanout() throws IOException {
        hashStore = new HttpHashStore(httpTransport, "fileFanouts", "chunkFanouts");
        expect(httpTransport.get("fileFanouts/abc")).andReturn(fanoutBytes);
        replayAll();
        Fanout fanout = hashStore.getFileFanout("abc");
        assertNotNull(fanout);
        Assert.assertEquals(1000, fanout.getActualContentLength());
        Assert.assertEquals(3, fanout.getHashes().size() );
        Assert.assertEquals("A1", fanout.getHashes().get(0));
        Assert.assertEquals("A3", fanout.getHashes().get(2));
    }

    @Test
    public void testGetChunkFanout() throws IOException {
        hashStore = new HttpHashStore(httpTransport, "fileFanouts", "chunkFanouts");
        expect(httpTransport.get("chunkFanouts/abc")).andReturn(fanoutBytes);
        replayAll();
        Fanout fanout = hashStore.getChunkFanout("abc");
        assertNotNull(fanout);
        Assert.assertEquals(1000, fanout.getActualContentLength());
        Assert.assertEquals(3, fanout.getHashes().size() );
        Assert.assertEquals("A1", fanout.getHashes().get(0));
        Assert.assertEquals("A3", fanout.getHashes().get(2));
    }
}
