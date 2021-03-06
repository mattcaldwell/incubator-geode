/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin.internal;

import static org.junit.Assert.*;

import com.gemstone.gemfire.admin.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.cache.*;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

import java.util.*;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Contains simple tests for the {@link CacheHealthEvaluator}
 *
 * @author David Whitlock
 *
 * @since 3.5
 */
@SuppressWarnings("deprecation")
@Category(IntegrationTest.class)
public class CacheHealthEvaluatorJUnitTest extends HealthEvaluatorTestCase {

  @Rule
  public TestName testName = new TestName();

  /**
   * Tests that we are in {@link GemFireHealth#OKAY_HEALTH okay}
   * health if cache loads take too long.
   *
   * @see CacheHealthEvaluator#checkLoadTime
   */
  @Test
  public void testCheckLoadTime() throws CacheException {
    Cache cache = CacheFactory.create(this.system);
    CachePerfStats stats = ((GemFireCacheImpl) cache).getCachePerfStats();

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setCacheLoader(new CacheLoader() {
        public Object load(LoaderHelper helper)
          throws CacheLoaderException {

          return "Loaded";
        }

        public void close() { }
      });

    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(getName(), attrs);
    
    GemFireHealthConfig config = new GemFireHealthConfigImpl(null);
    config.setMaxLoadTime(100);
    
    CacheHealthEvaluator eval =
      new CacheHealthEvaluator(config, this.system.getDistributionManager());
    for (int i = 0; i < 10; i++) {
      region.get("Test1 " + i);
    }
    long firstLoadTime = stats.getLoadTime();
    long firstLoadsCompleted = stats.getLoadsCompleted();
    assertTrue(firstLoadTime >= 0);
    assertTrue(firstLoadsCompleted > 0);

    // First time should always be empty
    List status = new ArrayList();
    eval.evaluate(status);
    assertEquals(0, status.size());

    config = new GemFireHealthConfigImpl(null);
    config.setMaxLoadTime(10);
    eval = new CacheHealthEvaluator(config,
                                    this.system.getDistributionManager());
    eval.evaluate(status);

    long start = System.currentTimeMillis();
    for (int i = 0; i < 100; i++) {
      region.get("Test2 " + i);
    }
    assertTrue(System.currentTimeMillis() - start < 1000);
    long secondLoadTime = stats.getLoadTime();
    long secondLoadsCompleted = stats.getLoadsCompleted();
    assertTrue("firstLoadTime=" + firstLoadTime + ", secondLoadTime=" + secondLoadTime, secondLoadTime >= firstLoadTime);
    assertTrue(secondLoadsCompleted > firstLoadsCompleted);

    // Averge should be less than 10 milliseconds
    status = new ArrayList();
    eval.evaluate(status);
    assertEquals(0, status.size());

    region.getAttributesMutator().setCacheLoader(new CacheLoader() {
        public Object load(LoaderHelper helper)
          throws CacheLoaderException {

          try {
            Thread.sleep(20);

          } catch (InterruptedException ex) {
            fail("Why was I interrupted?");
          }
          return "Loaded";
        }

        public void close() { }

      });

    for (int i = 0; i < 50; i++) {
      region.get("Test3 " + i);
    }

    long thirdLoadTime = stats.getLoadTime();
    long thirdLoadsCompleted = stats.getLoadsCompleted();
    assertTrue(thirdLoadTime > secondLoadTime);
    assertTrue(thirdLoadsCompleted > secondLoadsCompleted);

    status = new ArrayList();
    eval.evaluate(status);
    assertEquals(1, status.size());
    
    AbstractHealthEvaluator.HealthStatus ill =
      (AbstractHealthEvaluator.HealthStatus) status.get(0);
    assertEquals(GemFireHealth.OKAY_HEALTH, ill.getHealthCode());
    String s = "The average duration of a Cache load";
    assertTrue(ill.getDiagnosis().indexOf(s) != -1);
  }

  /**
   * Tests that we are in {@link GemFireHealth#OKAY_HEALTH okay}
   * health if the hit ratio dips below the threshold.
   */
  @Test
  public void testCheckHitRatio() throws CacheException {
    Cache cache = CacheFactory.create(this.system);
//    CachePerfStats stats = ((GemFireCache) cache).getCachePerfStats();

        AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setCacheLoader(new CacheLoader() {
        public Object load(LoaderHelper helper)
          throws CacheLoaderException {

          return "Loaded";
        }

        public void close() { }
      });

    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(getName(), attrs);
    
    GemFireHealthConfig config = new GemFireHealthConfigImpl(null);
    config.setMinHitRatio(0.5);

    CacheHealthEvaluator eval =
      new CacheHealthEvaluator(config,
                               this.system.getDistributionManager());
    List status = new ArrayList();
    eval.evaluate(status);
    assertEquals(0, status.size());

    region.get("One");
    region.get("One");
    region.get("One");

    status = new ArrayList();
    eval.evaluate(status);
    assertEquals(0, status.size());

    for (int i = 0; i < 50; i++) {
      region.get("Miss " + i);
    }

    status = new ArrayList();
    eval.evaluate(status);
    
    AbstractHealthEvaluator.HealthStatus ill =
      (AbstractHealthEvaluator.HealthStatus) status.get(0);
    assertEquals(GemFireHealth.OKAY_HEALTH, ill.getHealthCode());
    String s = "The hit ratio of this Cache";
    assertTrue(ill.getDiagnosis().indexOf(s) != -1);
  }

  private String getName() {
    return getClass().getSimpleName()+"_"+testName.getMethodName();
  }
}
