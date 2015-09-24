package com.gemstone.gemfire.internal.cache;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.cache.RemotePutMessage.PutReplyMessage;
import com.gemstone.gemfire.internal.offheap.NullOffHeapMemoryStats;
import com.gemstone.gemfire.internal.offheap.NullOutOfOffHeapMemoryListener;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.Chunk;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.DataAsAddress;
import com.gemstone.gemfire.internal.offheap.StoredObject;
import com.gemstone.gemfire.internal.offheap.UnsafeMemoryChunk;
import com.gemstone.gemfire.internal.util.BlobHelper;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class PutReplyMessageJUnitTest {
  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testValueSerialization() throws IOException, ClassNotFoundException {
    byte[] bytes = new byte[1024];
    HeapDataOutputStream hdos = new HeapDataOutputStream(bytes);
    PutReplyMessage imsg = new PutReplyMessage();

    // null value
    {
      PutReplyMessage omsg = new PutReplyMessage(1, true, Operation.PUT_IF_ABSENT, null, null, null);
      omsg.toData(hdos);
      imsg.fromData(new DataInputStream(new ByteArrayInputStream(bytes)));
      assertEquals(null, imsg.oldValue);
      assertEquals(null, imsg.getOldValue());
    }
    
    // simple byte array
    {
      byte[] baValue = new byte[] {1,2,3,4,5,6,7,8,9};
      PutReplyMessage omsg = new PutReplyMessage(1, true, Operation.PUT_IF_ABSENT, null, null, null);
      omsg.importOldBytes(baValue, false);
      hdos = new HeapDataOutputStream(bytes);
      omsg.toData(hdos);
      imsg.fromData(new DataInputStream(new ByteArrayInputStream(bytes)));
      assertArrayEquals(baValue, (byte[])imsg.oldValue);
      assertArrayEquals(baValue, (byte[])imsg.getOldValue());
    }
    
    // String in serialized form
    {
      String stringValue = "1,2,3,4,5,6,7,8,9";
      byte[] stringValueBlob = EntryEventImpl.serialize(stringValue);
      PutReplyMessage omsg = new PutReplyMessage(1, true, Operation.PUT_IF_ABSENT, null, null, null);
      omsg.importOldBytes(stringValueBlob, true);
      hdos = new HeapDataOutputStream(bytes);
      omsg.toData(hdos);
      imsg.fromData(new DataInputStream(new ByteArrayInputStream(bytes)));
      assertArrayEquals(stringValueBlob, ((VMCachedDeserializable)imsg.oldValue).getSerializedValue());
      assertArrayEquals(stringValueBlob, ((VMCachedDeserializable)imsg.getOldValue()).getSerializedValue());
    }
    
    // String in object form
    {
      String stringValue = "1,2,3,4,5,6,7,8,9";
      byte[] stringValueBlob = EntryEventImpl.serialize(stringValue);
      PutReplyMessage omsg = new PutReplyMessage(1, true, Operation.PUT_IF_ABSENT, null, null, null);
      omsg.importOldObject(stringValue, true);
      hdos = new HeapDataOutputStream(bytes);
      omsg.toData(hdos);
      imsg.fromData(new DataInputStream(new ByteArrayInputStream(bytes)));
      assertArrayEquals(stringValueBlob, ((VMCachedDeserializable)imsg.oldValue).getSerializedValue());
      assertArrayEquals(stringValueBlob, ((VMCachedDeserializable)imsg.getOldValue()).getSerializedValue());
    }
    
    // off-heap DataAsAddress byte array
    {
      SimpleMemoryAllocatorImpl sma =
          SimpleMemoryAllocatorImpl.create(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{new UnsafeMemoryChunk(1024*1024)});
      try {
        byte[] baValue = new byte[] {1,2};
        DataAsAddress baValueSO = (DataAsAddress) sma.allocateAndInitialize(baValue, false, false, null);
        PutReplyMessage omsg = new PutReplyMessage(1, true, Operation.PUT_IF_ABSENT, null, null, null);
        omsg.importOldObject(baValueSO, false);
        hdos = new HeapDataOutputStream(bytes);
        omsg.toData(hdos);
        imsg.fromData(new DataInputStream(new ByteArrayInputStream(bytes)));
        assertArrayEquals(baValue, (byte[])imsg.oldValue);
        assertArrayEquals(baValue, (byte[])imsg.getOldValue());
      } finally {
        SimpleMemoryAllocatorImpl.freeOffHeapMemory();
      }
    }
    // off-heap Chunk byte array
    {
      SimpleMemoryAllocatorImpl sma =
          SimpleMemoryAllocatorImpl.create(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{new UnsafeMemoryChunk(1024*1024)});
      try {
        byte[] baValue = new byte[] {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17};
        Chunk baValueSO = (Chunk) sma.allocateAndInitialize(baValue, false, false, null);
        PutReplyMessage omsg = new PutReplyMessage(1, true, Operation.PUT_IF_ABSENT, null, null, null);
        omsg.importOldObject(baValueSO, false);
        hdos = new HeapDataOutputStream(bytes);
        omsg.toData(hdos);
        imsg.fromData(new DataInputStream(new ByteArrayInputStream(bytes)));
        assertArrayEquals(baValue, (byte[])imsg.oldValue);
        assertArrayEquals(baValue, (byte[])imsg.getOldValue());
      } finally {
        SimpleMemoryAllocatorImpl.freeOffHeapMemory();
      }
    }
    // off-heap DataAsAddress String
    {
      SimpleMemoryAllocatorImpl sma =
          SimpleMemoryAllocatorImpl.create(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{new UnsafeMemoryChunk(1024*1024)});
      try {
        String baValue = "12";
        byte[] baValueBlob = BlobHelper.serializeToBlob(baValue);
        DataAsAddress baValueSO = (DataAsAddress) sma.allocateAndInitialize(baValueBlob, true, false, null);
        PutReplyMessage omsg = new PutReplyMessage(1, true, Operation.PUT_IF_ABSENT, null, null, null);
        omsg.importOldObject(baValueSO, true);
        hdos = new HeapDataOutputStream(bytes);
        omsg.toData(hdos);
        imsg.fromData(new DataInputStream(new ByteArrayInputStream(bytes)));
        assertArrayEquals(baValueBlob, ((VMCachedDeserializable)imsg.oldValue).getSerializedValue());
        assertArrayEquals(baValueBlob, ((VMCachedDeserializable)imsg.getOldValue()).getSerializedValue());
      } finally {
        SimpleMemoryAllocatorImpl.freeOffHeapMemory();
      }
    }
    // off-heap Chunk String
    {
      SimpleMemoryAllocatorImpl sma =
          SimpleMemoryAllocatorImpl.create(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{new UnsafeMemoryChunk(1024*1024)});
      try {
        String baValue = "12345678";
        byte[] baValueBlob = BlobHelper.serializeToBlob(baValue);
        Chunk baValueSO = (Chunk) sma.allocateAndInitialize(baValueBlob, true, false, null);
        PutReplyMessage omsg = new PutReplyMessage(1, true, Operation.PUT_IF_ABSENT, null, null, null);
        omsg.importOldObject(baValueSO, true);
        hdos = new HeapDataOutputStream(bytes);
        omsg.toData(hdos);
        imsg.fromData(new DataInputStream(new ByteArrayInputStream(bytes)));
        assertArrayEquals(baValueBlob, ((VMCachedDeserializable)imsg.oldValue).getSerializedValue());
        assertArrayEquals(baValueBlob, ((VMCachedDeserializable)imsg.getOldValue()).getSerializedValue());
      } finally {
        SimpleMemoryAllocatorImpl.freeOffHeapMemory();
      }
    }
  }
}
