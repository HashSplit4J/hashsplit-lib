package org.hashsplit4j.store;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import org.easymock.EasyMockSupport;
import org.hashsplit4j.api.BlobStore;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author dylan
 */
public class MigratingBlobStoreIT extends EasyMockSupport {

    BlobStore newBlobStore = createNiceMock(BlobStore.class);

    BlobStore oldBlobStore = createNiceMock(BlobStore.class);

    BlobStore migratingBlobStore;

    public MigratingBlobStoreIT() {
        migratingBlobStore = new MigratingBlobStore(newBlobStore, oldBlobStore);
    }

    /**
     * Test of getBlob method, of class MigratingBlobStore.
     */
    @Test
    public void testGetBlob() {
        expect(newBlobStore.hasBlob("hash22")).andReturn(true);
        expect(newBlobStore.getBlob("hash22")).andReturn("hash22".getBytes());
        expectLastCall();

        expect(newBlobStore.hasBlob("hash30")).andReturn(false);
        expect(oldBlobStore.hasBlob("hash30")).andReturn(Boolean.TRUE);
        expect(oldBlobStore.getBlob("hash30")).andReturn("hash30".getBytes());
        expectLastCall();

        replayAll();

        byte[] blob = migratingBlobStore.getBlob("hash22");
        Assert.assertEquals("hash22", new String(blob));

        byte[] blob2 = migratingBlobStore.getBlob("hash30");
        Assert.assertEquals("hash30", new String(blob2));
    }

    /**
     * Test of hasBlob method, of class MigratingBlobStore.
     */
    @Test
    public void testHasBlob() {
        expect(newBlobStore.hasBlob("hash1")).andReturn(true);
        expectLastCall();

        expect(newBlobStore.hasBlob("hash2")).andReturn(false);
        expect(oldBlobStore.hasBlob("hash2")).andReturn(true);
        expectLastCall();

        replayAll();

        boolean hash1 = migratingBlobStore.hasBlob("hash1");
        Assert.assertEquals(true, hash1);

        boolean hash2 = migratingBlobStore.hasBlob("hash2");
        Assert.assertEquals(true, hash2);
    }
}
