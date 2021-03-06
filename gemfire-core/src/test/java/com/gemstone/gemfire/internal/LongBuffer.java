/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal;

public class LongBuffer {

    private long data[];  // the array implementing the buffer
    public int length;  // number of valid elements in the buffer,
                 //   data[0.. this.length-1 ] are the valid elements

    /** construct a new instance of the specified capacity */
    LongBuffer(int size) {
      data = new long[size];
      length = 0;
    }

    /** construct a new instance, installing the argument as the data array*/
    LongBuffer(long[] someIds) {
      data = someIds;
      length = someIds.length;
    }

    /** change the capacity to the specified size, without loosing elements*/
    private void changeSize(int newSize) {
      if (newSize >= length) {  // only change size if we won't loose data
        long[] oldData = data;
        int oldLength = length;
        data = new long[newSize];
        length = 0;
        add(oldData, oldLength);
      }
    }


    public synchronized void add(long id) {
      if (length >= data.length) {
        // if buffer is large, don't double to reduce out-of-mem problems
        if (length > 10000)
          changeSize(length + 2000);
        else
          changeSize(length * 2);
      }
      data[length++] = id;
    }

    /** add argIds[0..argLength] , growing as required */
    public synchronized void add(long[] argIds, int argLength ) {
      if (length + argLength > data.length) {
        long[] oldData = data;
        data = new long[argLength + length];
        System.arraycopy(oldData, 0, data, 0, oldData.length);
      }
      System.arraycopy(argIds, 0, data, length, argLength);
      length += argLength;
    }

    /** add all of argIds, growing as required */
    public synchronized void add(long[] argIds) {
      add(argIds, argIds.length);
    }

    /** add all elements in the argument, growing as required */
    public synchronized void add(LongBuffer argBuf) {
      add(argBuf.data, argBuf.length);
    }

    public synchronized long get(int index) {
      if (index >= length)
        throw new IndexOutOfBoundsException(" index " + index + " length " + length);
      return data[index];
    }

    public synchronized long getAndStore(int index, int newValue) {
      if (index >= length)
        throw new IndexOutOfBoundsException(" index " + index + " length " + length);
      long result = data[index];
      data[index] = newValue;
      return result;
    }

    public synchronized void clear() { length = 0; }
}
