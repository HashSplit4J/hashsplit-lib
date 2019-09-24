/*
 * Copyright (C) McEvoy Software Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.hashsplit4j.api;

import java.util.List;
import org.bouncycastle.crypto.Digest;

public class Crypt {

    /**
     * Calculates the directory hash of the given string of contents
     *
     * @param text
     * @return HEX string
     */
    public static String toHexFromText(String text) {
        if (text == null) {
            return null;
        }

        return toHexFromByte(text.getBytes());
    }

    /**
     * Calculates the directory hash of the given bytes of contents
     *
     * @param bytes
     * @return HEX string
     */
    public static String toHexFromByte(byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        Digest crypto = Parser.getCrypt();
        for (byte b : bytes) {
            crypto.update(b);
        }
        return Parser.toHex(crypto);
    }

    /**
     * Calculates the hash of the given childrens (ie the directory hash with
     * the given childrens)
     *
     * @param childrens
     * @return HEX string
     */
    public static String toHexFromBlob(List<HashGroup> childrens) {
        Digest crypto = Parser.getCrypt();
        for (HashGroup child : childrens) {
            String line = toHashableText(child);

            for (byte b : line.getBytes()) {
                crypto.update(b);
            }
        }
        return Parser.toHex(crypto);
    }

    /**
     * Calculates the hash for the given hashes of Blobs (ie the directory hash
     * with the given blobs)
     *
     * @param childrens
     * @return HEX string
     */
    public static String toHexFromHash(List<String> childrens) {
        Digest crypto = Parser.getCrypt();
        for (String children : childrens) {
            String line = children;
            line += "\n";

            for (byte b : line.getBytes()) {
                crypto.update(b);
            }
        }
        return Parser.toHex(crypto);
    }

    /**
     * Calculates the hash for the given Sub Group for a Root group
     *
     * E.g: Sub Group {sub group's name} : {hash}
     * {012abc}:{7359c2e2759dae4b07d99643400a838eaa2e7a25} -> hash =
     * SHA1(012abc:7359c2e2759dae4b07d99643400a838eaa2e7a25 + '\n')
     *
     * @param child
     * @return
     */
    private static String toHashableText(HashGroup child) {
        StringBuilder builder = new StringBuilder();
        if (child == null) {
            return null;
        }

        builder.append(child.getName());
        builder.append(":");
        builder.append(child.getContentHash());
        builder.append("\n");
        return builder.toString();
    }
}
