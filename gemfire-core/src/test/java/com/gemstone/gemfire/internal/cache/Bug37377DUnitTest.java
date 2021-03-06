/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.io.File;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.lru.EnableLRU;
import com.gemstone.gemfire.internal.util.concurrent.CustomEntryConcurrentHashMap.HashEntry;

import dunit.AsyncInvocation;
import dunit.DistributedTestCase;
import dunit.Host;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * Bug37377 DUNIT Test: The Clear operation during a GII in progress can leave a
 * Entry in the Oplog due to a race condition wherein the clearFlag getting set
 * after the entry gets written to the disk, The Test verifies the existence of
 * the scenario.
 * 
 * @author pbatra
 */

public class Bug37377DUnitTest extends CacheTestCase
{

  protected static String regionName = "TestRegion";

  static Properties props = new Properties();

  protected static DistributedSystem distributedSystem = null;

  private static VM vm0 = null;

  private static VM vm1 = null;

  protected static Cache cache = null;

  protected static File[] dirs = null;

  private static final int maxEntries = 10000;

  /**
   * Constructor
   * 
   * @param name
   */
  public Bug37377DUnitTest(String name) {
    super(name);
    File file1 = new File(name + "1");
    file1.mkdir();
    file1.deleteOnExit();
    File file2 = new File(name + "2");
    file2.mkdir();
    file2.deleteOnExit();
    dirs = new File[2];
    dirs[0] = file1;
    dirs[1] = file2;
  }

  public void setUp() throws Exception
  {
    super.setUp();
    final Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);

  }

  public void tearDown2() throws Exception
  {

    vm1.invoke(destroyRegion());
    vm0.invoke(destroyRegion());

    super.tearDown2();
  }

  /**
   * This method is used to create Cache in VM0
   * 
   * @return CacheSerializableRunnable
   */

  private CacheSerializableRunnable createCacheForVM0()
  {
    SerializableRunnable createCache = new CacheSerializableRunnable(
        "createCache") {
      public void run2()
      {
        try {

          distributedSystem = (new Bug37377DUnitTest("vm0_diskReg"))
              .getSystem(props);
          assertTrue(distributedSystem != null);
          cache = CacheFactory.create(distributedSystem);
          assertTrue(cache != null);
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
          factory.setDiskSynchronous(false);
          factory.setDiskStoreName(cache.createDiskStoreFactory()
                                   .setDiskDirs(dirs)
                                   .create("Bug37377DUnitTest")
                                   .getName());
          RegionAttributes attr = factory.create();
          cache.createRegion(regionName, attr);
        }
        catch (Exception ex) {
          ex.printStackTrace();
          fail("Error Creating cache / region ");
        }
      }
    };
    return (CacheSerializableRunnable)createCache;
  }

  /**
   * This method is used to create Cache in VM1
   * 
   * @return CacheSerializableRunnable
   */
  private CacheSerializableRunnable createCacheForVM1()
  {
    SerializableRunnable createCache = new CacheSerializableRunnable(
        "createCache") {
      public void run2()
      {
        try {
          distributedSystem = (new Bug37377DUnitTest("vm1_diskReg"))
              .getSystem(props);
          assertTrue(distributedSystem != null);
          cache = CacheFactory.create(distributedSystem);
          assertTrue("cache found null", cache != null);

          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
          factory.setDiskSynchronous(false);
          factory.setDiskStoreName(cache.createDiskStoreFactory()
                                   .setDiskDirs(dirs)
                                   .create("Bug37377DUnitTest")
                                   .getName());
          RegionAttributes attr = factory.create();
          DistributedRegion distRegion = new DistributedRegion(regionName,
              attr, null, (GemFireCacheImpl)cache, new InternalRegionArguments()
                  .setDestroyLockFlag(true).setRecreateFlag(false)
                  .setSnapshotInputStream(null).setImageTarget(null));
//          assertTrue("Distributed Region is null", distRegion != null); (cannot be null)

          ((AbstractRegionMap)distRegion.entries)
              .setEntryFactory(TestAbstractDiskRegionEntry.getEntryFactory());

          LocalRegion region = (LocalRegion)((GemFireCacheImpl)cache)
              .createVMRegion(regionName, attr, new InternalRegionArguments()
                  .setInternalMetaRegion(distRegion).setDestroyLockFlag(true)
                  .setSnapshotInputStream(null).setImageTarget(null));
          assertTrue("Local Region is null", region != null);

        }
        catch (Exception ex) {
          ex.printStackTrace();
          fail("Error Creating cache / region " + ex);
        }
      }
    };
    return (CacheSerializableRunnable)createCache;
  }

  /**
   * This method puts in maxEntries in the Region
   * 
   * @return CacheSerializableRunnable
   */
  private CacheSerializableRunnable putSomeEntries()
  {
    SerializableRunnable puts = new CacheSerializableRunnable("putSomeEntries") {
      public void run2()
      {
        assertTrue("Cache is found as null ", cache != null);
        Region rgn = cache.getRegion(regionName);
        for (int i = 0; i < maxEntries; i++) {
          rgn.put(new Long(i), new Long(i));
        }
      }
    };
    return (CacheSerializableRunnable)puts;
  }

  /**
   * This method destroys the Region
   * 
   * @return CacheSerializableRunnable
   */
  private CacheSerializableRunnable destroyRegion()
  {
    SerializableRunnable puts = new CacheSerializableRunnable("destroyRegion") {
      public void run2()
      {
        try {
          assertTrue("Cache is found as null ", cache != null);

          Region rgn = cache.getRegion(regionName);
          rgn.localDestroyRegion();
          cache.close();
        }
        catch (Exception ex) {

        }
      }
    };
    return (CacheSerializableRunnable)puts;
  }

  /**
   * This method is used to close cache on the calling VM
   * 
   * @return CacheSerializableRunnable
   */
  private CacheSerializableRunnable closeCacheForVM(final int vmNo)
  {
    SerializableRunnable cclose = new CacheSerializableRunnable(
        "closeCacheForVM") {
      public void run2()
      {
        if (vmNo == 0) {
          cache.getRegion(regionName).localDestroyRegion();
        }
        assertTrue("Cache is found as null ", cache != null);
        cache.close();
      }
    };
    return (CacheSerializableRunnable)cclose;
  }

  /**
   * This method is used to close cache on the calling VM
   * 
   * @return CacheSerializableRunnable
   */
  private CacheSerializableRunnable closeCacheInVM()
  {
    SerializableRunnable cclose = new CacheSerializableRunnable(
        "closeCacheInVM") {
      public void run2()
      {

        cache.getRegion(regionName).localDestroyRegion();
        assertTrue("Cache is found as null ", cache != null);
        cache.close();
      }
    };
    return (CacheSerializableRunnable)cclose;
  }

  /**
   * This method verifies that the reintialized region size should be zero
   * 
   * @return CacheSerializableRunnable
   */
  private CacheSerializableRunnable verifyExtraEntryFromOpLogs()
  {
    SerializableRunnable verify = new CacheSerializableRunnable(
        "verifyExtraEntryFromOpLogs") {
      public void run2()
      {
        assertTrue("Cache is found as null ", cache != null);
        Region rgn = cache.getRegion(regionName);
        // should be zero after reinit
        assertEquals(0, rgn.size());
      }
    };
    return (CacheSerializableRunnable)verify;

  }

  /**
   * The Clear operation during a GII in progress can leave a Entry in the Oplog
   * due to a race condition wherein the clearFlag getting set after the entry
   * gets written to the disk, The Test verifies the existence of the scenario.
   * 
   */

  public void testGIIputWithClear()
  {
    vm0.invoke(createCacheForVM0());
    vm0.invoke(putSomeEntries());
    AsyncInvocation as1 = vm1.invokeAsync(createCacheForVM1());
    pause(10000);
    DistributedTestCase.join(as1, 30 * 1000, getLogWriter());
    vm0.invoke(closeCacheForVM(0));
    vm1.invoke(closeCacheForVM(1));
    vm1.invoke(createCacheForVM1());
    vm1.invoke(verifyExtraEntryFromOpLogs());
  }

  static class TestAbstractDiskRegionEntry extends VMThinDiskRegionEntryHeapObjectKey
  {
    protected TestAbstractDiskRegionEntry(RegionEntryContext r, Object key,
        Object value) {
      super(r, key, value);
    }
    
    private static RegionEntryFactory factory = new RegionEntryFactory() {
      public final RegionEntry createEntry(RegionEntryContext r, Object key,
          Object value)
      {

        return new TestAbstractDiskRegionEntry(r, key, value);
      }

      public final Class getEntryClass()
      {

        return TestAbstractDiskRegionEntry.class;
      }

      public RegionEntryFactory makeVersioned() {
        return this;
      }
      
      public RegionEntryFactory makeOnHeap() {
        return this;
      }
    };

    /**
     * Overridden setValue method to call clear Region before actually writing the
     * entry
     */
    @Override
    public boolean initialImageInit(final LocalRegion r,
                                    final long lastModifiedTime,
                                    final Object newValue,
                                    final boolean create,
                                    final boolean wasRecovered,
                                    final boolean versionTagAccepted) throws RegionClearedException
    {
      RegionEventImpl event = new RegionEventImpl(r, Operation.REGION_CLEAR,
                                                  null, true /* isOriginRemote */,
                                                  r.cache.getMyId());
      ((DistributedRegion)r).cmnClearRegion(event, false, false);
      boolean result = super.initialImageInit(r, lastModifiedTime, newValue, create, wasRecovered, versionTagAccepted);
      fail("expected RegionClearedException");
      return result;
    }

    public static RegionEntryFactory getEntryFactory()
    {
      return factory;
    }
  }
}
