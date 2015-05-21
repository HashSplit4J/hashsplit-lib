package org.hashsplit4j.utils;

import java.util.ArrayList;
import java.util.List;
import org.hashsplit4j.api.Fanout;
import org.hashsplit4j.api.FanoutImpl;

/**
 *
 * @author dylan
 */
public class StringFanoutUtils {

    public static String formatFanout(List<String> blobHashes, long actualContentLength) {
        StringBuilder sb = new StringBuilder();

        for (String hash : blobHashes) {
            sb.append(hash).append(",");
        }

        sb.append(actualContentLength);

        return sb.toString();
    }

    public static Fanout parseFanout(String fan) {
        String[] parts = fan.split(",");
        List<String> blobHashes = new ArrayList<>();
        Long actualContentLength = null;
        int len = parts.length;
        int count = 0;
        for (String part : parts) {
            if (++count < len) {
                blobHashes.add(part);
            } else {
                actualContentLength = Long.valueOf(part);
            }
        }
        return new FanoutImpl(blobHashes, actualContentLength);
    }
}
