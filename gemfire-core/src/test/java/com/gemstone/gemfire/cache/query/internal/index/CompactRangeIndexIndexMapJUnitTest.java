/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.internal.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.util.HashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.data.Position;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class CompactRangeIndexIndexMapJUnitTest {

  
  @Before
  public void setUp() throws Exception {
    System.setProperty("gemfire.Query.VERBOSE", "true");
    CacheUtils.startCache();
  }

  @After
  public void tearDown() throws Exception {
    System.setProperty("gemfire.Query.VERBOSE", "false");
    CacheUtils.closeCache();
  }

  @Test
  public void testCreateFromEntriesIndex() {
    
  }
  
  @Test
  public void testCreateIndexAndPopulate() {
    
  }
  
  @Test
  public void testLDMIndexCreation() throws Exception {
    Cache cache = CacheUtils.getCache();
    Region region = createLDMRegion("portfolios");
    QueryService queryService = cache.getQueryService();
    Index index = queryService.createIndex("IDIndex", "p.ID", "/portfolios p, p.positions ps");
    assertTrue(index instanceof CompactRangeIndex);
  }
  
  @Test
  public void testFirstLevelEqualityQuery() throws Exception {
    testIndexAndQuery("p.ID", "/portfolios p", "Select * from /portfolios p where p.ID = 1");
    testIndexAndQuery("p.ID", "/portfolios p", "Select * from /portfolios p where p.ID > 1");
    testIndexAndQuery("p.ID", "/portfolios p", "Select * from /portfolios p where p.ID < 10");
  }
  
  @Test
  public void testSecondLevelEqualityQuery() throws Exception {
    boolean oldTestLDMValue = IndexManager.IS_TEST_LDM;
    boolean oldTestExpansionValue = IndexManager.IS_TEST_EXPANSION;
    testIndexAndQuery("p.ID", "/portfolios p, p.positions.values ps", "Select * from /portfolios p where p.ID = 1");
    testIndexAndQuery("p.ID", "/portfolios p, p.positions.values ps", "Select p.ID from /portfolios p where p.ID = 1");
    testIndexAndQuery("p.ID", "/portfolios p, p.positions.values ps", "Select p from /portfolios p where p.ID > 3");
    testIndexAndQuery("p.ID", "/portfolios p, p.positions.values ps", "Select ps from /portfolios p, p.positions.values ps where ps.secId = 'VMW'");
    IndexManager.IS_TEST_LDM = oldTestLDMValue;
    IndexManager.IS_TEST_EXPANSION = oldTestExpansionValue;
  }
  
  @Test
  public void testMultipleSecondLevelMatches() throws Exception {
    boolean oldTestLDMValue = IndexManager.IS_TEST_LDM;
    boolean oldTestExpansionValue = IndexManager.IS_TEST_EXPANSION;
    testIndexAndQuery("ps.secId", "/portfolios p, p.positions.values ps", "Select * from /portfolios p, p.positions.values ps where ps.secId = 'VMW'");
    IndexManager.IS_TEST_LDM = oldTestLDMValue;
    IndexManager.IS_TEST_EXPANSION = oldTestExpansionValue;
  }
  
  //executes queries against both no index and ldm index
  //compares size counts of both and compares results
  private void testIndexAndQuery(String indexExpression, String regionPath, String queryString) throws Exception {
    Cache cache = CacheUtils.getCache();
    int numEntries = 20;
    QueryService queryService = cache.getQueryService();
    IndexManager.IS_TEST_LDM = false;
    IndexManager.IS_TEST_EXPANSION = false;
    Region region = createReplicatedRegion("portfolios");
    createPortfolios(region, numEntries);
    
    //Test no index
    //Index index = queryService.createIndex("IDIndex", indexExpression, regionPath);
    Query query = queryService.newQuery(queryString);
    SelectResults noIndexResults = (SelectResults) query.execute();
    //clean up
    queryService.removeIndexes();
    
    //creates indexes that may be used by the queries
    Index index = queryService.createIndex("IDIndex", indexExpression, regionPath);
    query = queryService.newQuery(queryString);
    SelectResults memResults = (SelectResults) query.execute();
    //clean up
    queryService.removeIndexes();
    region.destroyRegion();
    
    //Now execute against a replicated region with regular indexes
    //we want to make sure we don't create and LDM index so undo the test hook
    IndexManager.IS_TEST_LDM = true;
    IndexManager.IS_TEST_EXPANSION = true;
    region = createLDMRegion("portfolios");
    createPortfolios(region, numEntries);

    index = queryService.createIndex("IDIndex", indexExpression, regionPath);
    query = queryService.newQuery(queryString);
    SelectResults ldmResults = (SelectResults) query.execute();
    
    assertEquals("Size for no index and index results should be equal", noIndexResults.size(), memResults.size());
    assertEquals("Size for memory and ldm index results should be equal", memResults.size(), ldmResults.size());
    CacheUtils.log("Size is:" + memResults.size());
    //now check elements for both
    for (Object o: ldmResults) {
      assertTrue(memResults.contains(o));
    }
    queryService.removeIndexes();
    region.destroyRegion();
    
  }
  
  
  //Should be changed to ldm region
  //Also should remove IS_TEST_LDM when possible
  private Region createLDMRegion(String regionName) throws ParseException {
    IndexManager.IS_TEST_LDM = true;
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes regionAttributes = attributesFactory.create();
    return cache.createRegion(regionName, regionAttributes);
  }
  
  private Region createReplicatedRegion(String regionName) throws ParseException {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes regionAttributes = attributesFactory.create();
    return cache.createRegion(regionName, regionAttributes);
  }
  
  private void createPortfolios(Region region, int num) {
    for (int i = 0; i < num; i++) {
      Portfolio p = new Portfolio(i);
      p.positions = new HashMap();
      p.positions.put("VMW", new Position("VMW", Position.cnt * 1000));
      p.positions.put("IBM", new Position("IBM", Position.cnt * 1000));
      p.positions.put("VMW_2", new Position("VMW", Position.cnt * 1000));
      region.put("" + i, p);
    }
  }
  
}
