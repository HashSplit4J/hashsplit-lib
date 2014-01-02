/*
 */
package org.hashsplit4j.store;

import org.hashsplit4j.triplets.HashCalc;
import org.hashsplit4j.triplets.ITriplet;
import org.hashsplit4j.triplets.Triplet;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.io.IOUtils;

/**
 *
 * @author brad
 */
public class LocalHashManager {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(LocalHashManager.class);
    
    public static final Charset UTF8 = Charset.forName("UTF-8");

    private final HashCalc hashCalc = HashCalc.getInstance();

    private long lastWriteTime;

    public String getDirHash(File dir) throws IOException {
        File hashFile = new File(dir, ".hash");
        String dirHash;
        if (!hashFile.exists()) {
            List<ITriplet> triplets = getTriplets(dir);
            dirHash = writeHashes(dir, triplets);
        } else {
            // read first line of hash file, should only be one line
            try (Reader r = new FileReader(hashFile)) {
                try (BufferedReader bufferedReader = new BufferedReader(r)) {
                    dirHash = bufferedReader.readLine();
                }
            }
        }
        return dirHash;
    }

    public void writeTripletsToStream(File dir, OutputStream out) throws IOException {
        File hashes = new File(dir, ".hashes");
        if (hashes.exists()) {
            try (FileInputStream fin = new FileInputStream(hashes)) {
                try (BufferedInputStream bufIn = new BufferedInputStream(fin)) {
                    IOUtils.copyLarge(bufIn, out); // the hashes file is already in the required format so just copy it out
                }
            }
        } else {
            List<ITriplet> list = getTriplets(dir);
            hashCalc.calcHash(list, out);
        }
    }

    private List<ITriplet> getTriplets(File dir) throws IOException {
        List<ITriplet> list = new ArrayList<>();
        for (File child : dir.listFiles()) {
            if (!child.getName().startsWith(".")) {
                Triplet t = new Triplet();
                t.setName(child.getName());
                if (child.isFile()) {
                    t.setType("f");
                    t.setHash(child.getName()); // for blobs, the name is the hash
                } else {
                    String childHash = getDirHash(child);
                    t.setType("d");
                    t.setHash(childHash);
                }
                list.add(t);
            }
        }
        Collections.sort(list, new Comparator<ITriplet>() {

            @Override
            public int compare(ITriplet o1, ITriplet o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });
        return list;
    }

    private String writeHashes(File dir, List<ITriplet> triplets) throws FileNotFoundException, IOException {
        long sysTime = System.currentTimeMillis();
        long tm = sysTime - lastWriteTime;
        if (tm > 1000) {
            // Only log every second or so to minimise logging
            log.info("writeHashes: " + dir.getAbsolutePath());
        }
        lastWriteTime = sysTime;
        File hashes = new File(dir, ".hashes");
        String dirHash;
        try (FileOutputStream fout = new FileOutputStream(hashes, false)) {
            dirHash = hashCalc.calcHash(triplets, fout);
        }
        File hashFile = new File(dir, ".hash");
        try (FileOutputStream fout = new FileOutputStream(hashFile)) {
            fout.write(dirHash.getBytes(UTF8));
        }
        return dirHash;
    }

}
