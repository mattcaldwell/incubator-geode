/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.diskPerf;

import java.util.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.cache.DiskRegionHelperFactory;
import com.gemstone.gemfire.internal.cache.DiskRegionProperties;
import com.gemstone.gemfire.internal.cache.DiskRegionTestingBase;
import com.gemstone.gemfire.internal.cache.lru.LRUStatistics;
import com.gemstone.gemfire.internal.cache.*;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Disk region Perf test for Overflow only with Sync writes. 1) Performance of
 * Put operation causing an eviction. 2) Performance of Get operation for entry
 * which will fault in.
 */
@Category(IntegrationTest.class)
public class DiskRegOverflowSyncJUnitPerformanceTest extends DiskRegionTestingBase
{

  LogWriter log = null;

  static int counter = 0;

  DiskRegionProperties diskProps = new DiskRegionProperties();

  @Before
  public void setUp() throws Exception
  {
    super.setUp();
    diskProps.setDiskDirs(dirs);
    diskProps.setOverFlowCapacity(1000);
    region = DiskRegionHelperFactory
        .getSyncOverFlowOnlyRegion(cache, diskProps);

    log = ds.getLogWriter();
  }

  @After
  public void tearDown() throws Exception
  {
    super.tearDown();
    if (cache != null) {
      cache.close();
    }
    if (ds != null) {
      ds.disconnect();
    }
  }

 
  private static int ENTRY_SIZE = 1024 * 5;

  /* Do not change the value OP_COUNT = 400
   * The test case is dependent on this value.
   */
  
  private static int OP_COUNT = 400;

  private static int HALF_OP_COUNT = OP_COUNT / 2;

  @Test
  public void testPopulatefor5Kbwrites()
  {
//    RegionAttributes ra = region.getAttributes();

    LRUStatistics lruStats = getLRUStats(region);
    // Put in larger stuff until we start evicting
    int total;
    for (total = 0; lruStats.getEvictions() <= 0; total++) {
      log.info("DEBUG: total " + total + ", evictions "
          + lruStats.getEvictions());
      int[] array = new int[250];
      array[0] = total;
      region.put(new Integer(total), array);
    }

      assertEquals(1, lruStats.getEvictions());

    // put another 1mb data which will evicted to disk.
//    final String key = "K";
    final byte[] value = new byte[ENTRY_SIZE];
    Arrays.fill(value, (byte)77);

    for (int i = 0; i < HALF_OP_COUNT; i++) {
      log.info("DEBUG: total " + total + ", evictions "
          + lruStats.getEvictions());
      region.put("" + i, value);
    }

     assertEquals(201, lruStats.getEvictions());

    //the next puts will be written to disk
    long startTime = System.currentTimeMillis();
    for (int i = 201; i < OP_COUNT; i++) {
      region.put("" + i, value);
    }
    long endTime = System.currentTimeMillis();
    System.out.println(" done with putting");
    //Now get all the entries which are on disk.
    long startTimeGet = System.currentTimeMillis();
    for (int i = 0; i < HALF_OP_COUNT; i++) {
      region.get("" + i);
    }
    long endTimeGet = System.currentTimeMillis();
    System.out.println(" done with getting");

    region.close(); // closes disk file which will flush all
    // buffers
    float et = endTime - startTime;
    float etSecs = et / 1000f;
    float opPerSec = etSecs == 0 ? 0 : (OP_COUNT / (et / 1000f));
    float bytesPerSec = etSecs == 0 ? 0
        : ((OP_COUNT * ENTRY_SIZE) / (et / 1000f));

    String stats = "et=" + et + "ms writes/sec=" + opPerSec + " bytes/sec="
        + bytesPerSec;
    log.info(stats);
    System.out
        .println("Stats for 5kb writes: Perf of Put which is cauing eviction :"
            + stats);
    // Perf stats for get op
    float etGet = endTimeGet - startTimeGet;
    float etSecsGet = etGet / 1000f;
    float opPerSecGet = etSecsGet == 0 ? 0 : (OP_COUNT / (etGet / 1000f));
    float bytesPerSecGet = etSecsGet == 0 ? 0
        : ((OP_COUNT * ENTRY_SIZE) / (etGet / 1000f));

    String statsGet = "etGet=" + etGet + "ms gets/sec=" + opPerSecGet
        + " bytes/sec=" + bytesPerSecGet;
    log.info(statsGet);
    System.out.println("Perf Stats of get which is fauting in :" + statsGet);

  }

  protected LRUStatistics getLRUStats(Region region)
  {
    return ((LocalRegion)region).getEvictionController().getLRUHelper()
        .getStats();

  }

}
