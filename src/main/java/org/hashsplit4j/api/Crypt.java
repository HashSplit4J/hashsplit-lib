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

import java.security.MessageDigest;
import java.util.List;

public class Crypt {

  /**
   * Calculates the directory hash of the given string of contents
   * 
   * @param content
   * @return
   */
  public static String toHexFromText(String text) {
    if (text == null)
      return null;
    
    return toHexFromByte(text.getBytes());
  }
  
  /**
   * Calculates the directory hash of the given bytes of contents
   * 
   * @param content
   * @return
   */
  public static String toHexFromByte(byte[] bytes) {
    if (bytes == null)
      return null;

    MessageDigest crypto = Parser.getCrypt();
    crypto.update(bytes);
    return Parser.toHex(crypto);
  }

  /**
   * Calculates the hash of the given childrens (ie the directory hash with the given childrens)
   * 
   * @param children
   * @return
   */
  public static String toHexFromArray(List<Blob> childrens) {
    MessageDigest crypto = Parser.getCrypt();
    for (Blob child : childrens) {
      String line = toHashableText(child);
      crypto.update(line.getBytes());
    }
    return Parser.toHex(crypto);
  }
  
  public static String toHashableText(Blob child) {
    StringBuilder builder = new StringBuilder();
    if (child == null)
      return null;
    
    builder.append(child.getHash());
    builder.append(":");
    builder.append(child.getContents());
    builder.append("\n");
    return builder.toString();
  }
}
