/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Test of interrupting threads doing disk writes to see the effect.
 *
 */
@Category(IntegrationTest.class)
public class InterruptDiskJUnitTest  {

  private static volatile Thread puttingThread;
  private static final long MAX_WAIT = 60 * 1000;
  private DistributedSystem ds;
  private Cache cache;
  private Region<Object, Object> region;
  private ExecutorService ex;
  private AtomicLong nextValue = new AtomicLong();

  @Test
  @Ignore
  public void testLoop() throws Throwable {
    for(int i=0; i < 100; i++) {
      System.err.println("i=" +i);
      System.out.println("i=" +i);
      testDRPutWithInterrupt();
      tearDown();
      setUp();
    }
  }


  @Before
  public void setUp() {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.setProperty("log-level", "config"); // to keep diskPerf logs smaller
    props.setProperty("statistic-sampling-enabled", "true");
    props.setProperty("enable-time-statistics", "true");
    props.setProperty("statistic-archive-file", "stats.gfs");
    ds = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds);
    File diskStore = new File("diskStore");
    diskStore.mkdir();
    cache.createDiskStoreFactory().setMaxOplogSize(1).setDiskDirs(new File[] {diskStore} ).create("store");
    region = cache.createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT).setDiskStoreName("store").create("region");
    ex = Executors.newSingleThreadExecutor();
  }


  @After
  public void tearDown() {
    ds.disconnect();
    ex.shutdownNow();
  }


  @Test
  public void testDRPutWithInterrupt() throws Throwable {
    Callable doPuts = new Callable() {

      @Override
      public Object call() {
        puttingThread = Thread.currentThread();
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(MAX_WAIT);
        while(!Thread.currentThread().isInterrupted()) {
          region.put(0, nextValue.incrementAndGet());
          if(System.nanoTime() > end) {
            fail("Did not get interrupted in 60 seconds");
          }
        }
        return null;
      }
    };
    
    Future result = ex.submit(doPuts);
    
    
    Thread.sleep(50);
    long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(MAX_WAIT);
    while(puttingThread == null) {
      Thread.sleep(50);
      if(System.nanoTime() > end) {
        fail("Putting thread not set in 60 seconds");
      }
    }

    puttingThread.interrupt();
    
    result.get(60, TimeUnit.SECONDS);
    
    assertEquals(nextValue.get(), region.get(0));
    
  }
}