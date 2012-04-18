/* Rsum.java

   Rsum: A simple, "rolling" checksum based on Adler32
   Copyright (C) 2011 Tomas Hlavnicka <hlavntom@fel.cvut.cz>

   This file is a part of Jazsync.

   Jazsync is free software; you can redistribute it and/or modify it
   under the terms of the GNU General Public License as published by the
   Free Software Foundation; either version 2 of the License, or (at
   your option) any later version.

   Jazsync is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Jazsync; if not, write to the

      Free Software Foundation, Inc.,
      59 Temple Place, Suite 330,
      Boston, MA  02111-1307
      USA
 */

package org.hashsplit4j;

/**
 * Implementation of rolling checksum for zsync purposes
 * @author Tomáš Hlavni�?ka
 */
public class Rsum implements Cloneable, java.io.Serializable {
    private short a;
    private short b;
    private int oldByte;
    private int blockLength;
    private byte[] buffer;

    /**
     * Constructor of rolling checksum
     */
    public Rsum(int size){
        buffer = new byte[size+1];
        blockLength = size;
        a = b = 0;
        oldByte  = 0;
    }

    /**
     * Return the value of the currently computed checksum.
     *
     * @return The currently computed checksum.
     */
    public int getValue() {
        return ((a & 0xffff) | (b << 16));
    }

    /**
     * Reset the checksum
     */
    public void reset() {
        a = b = 0;
        oldByte = 0;
    }

    /**
     * Rolling checksum that takes single byte and compute checksum
     * of block from file in offset that equals offset of newByte 
     * minus length of block
     * 
     * @param newByte New byte that will actualize a checksum
     */
    public void roll(byte newByte) {
        short oldUnsignedB=unsignedByte(buffer[oldByte]);
        a -= oldUnsignedB;
        b -= blockLength * oldUnsignedB;
        a += unsignedByte(newByte);
        b += a;
        buffer[oldByte]=newByte;
        oldByte++;
        if(oldByte==blockLength){
            oldByte=0;
        }
    }

    /**
     * Update the checksum with an entirely different block, and
     * potentially a different block length.
     *
     * @param buf The byte array that holds the new block.
     * @param offset From whence to begin reading.
     * @param length The length of the block to read.
     */
    public void check(byte[] buf, int offset, int length) {
        reset();
        int index=offset;
        short unsignedB;
        for(int i=length;i>0;i--){
            unsignedB=unsignedByte(buf[index]);
            a+=unsignedB;
            b+=i*unsignedB;
            index++;
        }
    }


    /**
     * Returns "unsigned" value of byte
     *
     * @param b Byte to convert
     * @return Unsigned value of byte <code>b</code>
     */
    private short unsignedByte(byte b){
        if(b<0) {
            return (short)(b+256);
        }
        return b;
    }

    @Override
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException cnse) {
            throw new Error();
        }
    }

    @Override
    public boolean equals(Object o) {
        return ((Rsum) o).a == a && ((Rsum) o).b == b;
    }
}

