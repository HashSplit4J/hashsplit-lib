package org.hashsplit4j.store;

import java.io.File;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author brad
 */
public class SimpleFileDbTest {

    public SimpleFileDbTest() {
    }

    @Test
    public void testSave() throws Exception {
        File keysFile = new File("/tmp/dbkeys");
        File valuesFile = new File("/tmp/dbvals");

        keysFile.delete();
        valuesFile.delete();

        SimpleFileDb db = new SimpleFileDb(keysFile, valuesFile);
        byte[] arr1 = "Hello world!!!!!".getBytes();
        db.save("hello1", arr1);

        byte[] arr2 = db.load("hello1");
        Assert.assertEquals(arr1.length, arr2.length);

        db.save("hello2", "Another string 2".getBytes());
        db.save("hello3", "Another string 3".getBytes());


        SimpleFileDb db2 = new SimpleFileDb(keysFile, valuesFile);
        arr2 = db.load("hello1");
        Assert.assertEquals(arr1.length, arr2.length);


    }

}
