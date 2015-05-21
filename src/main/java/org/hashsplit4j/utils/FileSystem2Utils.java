package org.hashsplit4j.utils;

import java.io.File;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author dylan
 */
public class FileSystem2Utils {

    public static final int DEFAULT_SPLIT = 3;

    public static File toFile(File root, String hash) {
        return toFile(root, hash, DEFAULT_SPLIT);
    }

    public static File toFile(File root, String hash, final int splitLength) {
        return toFileWithPrefix(root, hash, null, splitLength);
    }

    public static File toFileWithPrefix(final File root, final String hash, final String dirPrefix) {
        return toFileWithPrefix(root, hash, dirPrefix, DEFAULT_SPLIT);
    }

    public static File toFileWithPrefix(final File root, final String hash, final String dirPrefix, final int splitLength) {
        File f = new File(root, (StringUtils.isNotEmpty(dirPrefix) ? dirPrefix : ""));
        String name = hash;
        String hex = hash;
        while (hex.length() > splitLength) {
            String subdir = hex.substring(0, splitLength);
            f = new File(f, subdir);
            hex = hex.substring(splitLength);
        }
        return new File(f, name);
    }
}
