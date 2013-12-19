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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class FileUtils {
    
	/**
	 * Read the given binary file, and return its contents as a byte array
	 * 
	 * @param file
	 * 			- a file with absolute path
	 * @return
	 * 			- its contents as a byte array
	 * @throws FileNotFoundException
	 * 			- if no such file or directory
	 */
	public static byte[] read(File file) {
		BufferedInputStream bis = null;
		byte[] buffer = new byte[1024];
		long checkSum = 0L;
		int nRead;
		
		try {
		    bis = new BufferedInputStream(new FileInputStream(file));
			while ((nRead = bis.read(buffer, 0, buffer.length)) != -1) {
				for (int i = 0; i < nRead; i++) {
					checkSum += buffer[i];
				}
			}
		} catch (FileNotFoundException ex) {
		    System.err.println("The file " + file.getAbsolutePath() + " does not exist");
		} catch (IOException ex) {
		    System.err.println("Could not read contents for the give file " + file.getAbsolutePath());
		} finally {
			try {
				// Releases any system resources associated with the stream
				if (bis != null)
				    bis.close();
			} catch (IOException ex) {
			    System.err.println("The buffered reader is already closed");
			}
		}
		
		return buffer;
	}
	
	/**
	 * SHA1 verifier
	 * 
	 * @param text
	 * 			- the text want to check is SHA1 or not
	 * @return
	 * 			- TRUE/FALSE
	 */
	public static boolean isSHA1(String text) {
		if (text == null || text.length() < 0)
			return false;
		
		return text.matches("[a-fA-F0-9]{40}");
	}
}
