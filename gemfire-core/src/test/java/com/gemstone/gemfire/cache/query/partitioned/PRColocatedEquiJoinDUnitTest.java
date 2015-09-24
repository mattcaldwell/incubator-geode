/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.cache.query.partitioned;

import java.util.ArrayList;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.NewPortfolio;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.partitioned.PRQueryDUnitHelper.TestQueryFunction;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDUnitTestCase;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import dunit.Host;
import dunit.TestException;
import dunit.VM;

/**
 * @author shobhit
 *
 */
public class PRColocatedEquiJoinDUnitTest extends PartitionedRegionDUnitTestCase {

  int totalNumBuckets = 100;

  int queryTestCycle = 10;

  PRQueryDUnitHelper PRQHelp = new PRQueryDUnitHelper("");

  final String name = "Portfolios1";

  final String coloName = "Portfolios2";

  final String localName = "LocalPortfolios1";

  final String coloLocalName = "LocalPortfolios2";
  
  final int cnt = 0, cntDest = 200;

  final int redundancy = 1;

  /**
   * @param name
   */
  public PRColocatedEquiJoinDUnitTest(String name) {
    super(name);
  }

  /**
   * A very basic dunit test that <br>
   * 1. Creates two PR Data Stores with redundantCopies = 1.
   * 2. Populates the region with test data.
   * 3. Fires a LOCAL query on one data store VM and verifies the result. 
   * @throws Exception
   */
  public void testPRLocalQuerying() throws Exception
  {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, Portfolio.class));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRColocatedCreate(coloName,
        redundancy, name));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName, NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = PRQHelp.createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = PRQHelp.createNewPortfoliosAndPositions(cntDest);
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));
    
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio,
        cnt, cntDest));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");
    
    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRColocatedDataSetQueryAndCompareResults(name, coloName, localName, coloLocalName));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  public void testNonColocatedPRLocalQuerying() throws Exception
  {
    addExpectedException("UnsupportedOperationException");
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, Portfolio.class));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    // Create second PR which is not colocated.
    vm0.invoke(new CacheSerializableRunnable(coloName) {
      @Override
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        Region partitionedregion = null;
        try {
          AttributesFactory attr = new AttributesFactory();
          attr.setValueConstraint(NewPortfolio.class);

          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          PartitionAttributes prAttr = paf.setRedundantCopies(redundancy)
              .create();

          attr.setPartitionAttributes(prAttr);

          partitionedregion = cache.createRegion(coloName, attr.create());
        }
        catch (IllegalStateException ex) {
          getLogWriter()
              .warning(
                  "PRQueryDUnitHelper#getCacheSerializableRunnableForPRCreateWithRedundancy: Creation caught IllegalStateException",
                  ex);
        }
        assertNotNull(
            "PRQueryDUnitHelper#getCacheSerializableRunnableForPRCreateWithRedundancy: Partitioned Region "
                + coloName + " not in cache", cache.getRegion(coloName));
        assertNotNull(
            "PRQueryDUnitHelper#getCacheSerializableRunnableForPRCreateWithRedundancy: Partitioned Region ref null",
            partitionedregion);
        assertTrue(
            "PRQueryDUnitHelper#getCacheSerializableRunnableForPRCreateWithRedundancy: Partitioned Region ref claims to be destroyed",
            !partitionedregion.isDestroyed());
      }
    });

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName, NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = PRQHelp.createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = PRQHelp.createNewPortfoliosAndPositions(cntDest);
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));
    
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio,
        cnt, cntDest));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");
    
    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(new CacheSerializableRunnable("PRQuery") {
      @Override
      public void run2() throws CacheException {

     // Helper classes and function
        final class TestQueryFunction extends FunctionAdapter {

          @Override
          public boolean hasResult() {
            return true;
          }

          @Override
          public boolean isHA() {
            return false;
          }

          private final String id;

          public TestQueryFunction(String id) {
            super();
            this.id = id;
          }

          @Override
          public void execute(FunctionContext context) {
            Cache cache = CacheFactory.getAnyInstance();
            QueryService queryService = cache.getQueryService();
            ArrayList allQueryResults = new ArrayList();
            String qstr = (String) context.getArguments();
            try {
              Query query = queryService.newQuery(qstr);
              context.getResultSender().sendResult((ArrayList) ((SelectResults) query
                      .execute((RegionFunctionContext) context)).asList());
              context.getResultSender().lastResult(null);
            } catch (Exception e) {
              e.printStackTrace();
              throw new FunctionException(e);
            }
          }

          @Override
          public String getId() {
            return this.id;
          }
        }
        Cache cache = getCache();
        // Querying the PR region
        
        String[] queries = new String[] {
            "r1.ID = r2.id",
            };

        Object r[][] = new Object[queries.length][2];
        Region region = null;
        region = cache.getRegion(name);
        assertNotNull(region);
        region = cache.getRegion(coloName);
        assertNotNull(region);
        
        QueryService qs = getCache().getQueryService();
        Object[] params;
        try {
          for (int j = 0; j < queries.length; j++) {
            getCache().getLogger().info(
                "About to execute local query: " + queries[j]);
            Function func = new TestQueryFunction("testfunction");
            
            Object funcResult = FunctionService.onRegion((getCache().getRegion(name) instanceof PartitionedRegion)? getCache().getRegion(name) : getCache().getRegion(coloName)).withArgs(
                "Select " + (queries[j].contains("ORDER BY") ? "DISTINCT" : "")
                    + " * from /" + name + " r1, /" + coloName
                    + " r2 where " + queries[j]).execute(func).getResult();

            r[j][0] = ((ArrayList)funcResult).get(0);
          }
          fail("PRQueryDUnitHelper#getCacheSerializableRunnableForPRQueryAndCompareResults: Queries Executed successfully with non-colocated region on one of the nodes");

        } catch (FunctionException e) {
          if (e.getCause() instanceof UnsupportedOperationException) {
            getLogWriter().info("Query received FunctionException successfully while using QueryService.");
          } else {
            fail("UnsupportedOperationException must be thrown here");
          }
        }
      }
    });

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  /**
   * A very basic dunit test that <br>
   * 1. Creates two PR Data Stores with redundantCopies = 1.
   * 2. Populates the region with test data.
   * 3. Fires a LOCAL query on one data store VM and verifies the result. 
   * @throws Exception
   */
  public void testPRLocalQueryingWithIndexes() throws Exception {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, Portfolio.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex1", "r1.ID", "/"+name+" r1", null));
    //vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex11", "r1.status", "/"+name+" r1", null));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRColocatedCreate(coloName,
        redundancy, name));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex2", "r2.id", "/"+coloName+" r2", null));
    //vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex22", "r2.status", "/"+coloName+" r2", null));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName, NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = PRQHelp.createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = PRQHelp.createNewPortfoliosAndPositions(cntDest);
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));
    
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio,
        cnt, cntDest));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");
    
    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRColocatedDataSetQueryAndCompareResults(name, coloName, localName, coloLocalName));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  /**
   * A very basic dunit test that <br>
   * 1. Creates two PR Data Stores with redundantCopies = 1.
   * 2. Populates the region with test data.
   * 3. Fires a LOCAL query on one data store VM and verifies the result. 
   * @throws Exception
   */
  public void testPRLocalQueryingWithIndexOnOneRegion() throws Exception {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, Portfolio.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex1", "r1.ID", "/"+name+" r1", null));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex11", "r1.status", "/"+name+" r1", null));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRColocatedCreate(coloName,
        redundancy, name));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName, NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = PRQHelp.createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = PRQHelp.createNewPortfoliosAndPositions(cntDest);
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));
    
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio,
        cnt, cntDest));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");
    
    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRColocatedDataSetQueryAndCompareResults(name, coloName, localName, coloLocalName));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  /**
   * A very basic dunit test that <br>
   * 1. Creates two PR Data Stores with redundantCopies = 1.
   * 2. Populates the region with test data.
   * 3. Fires a LOCAL query on one data store VM and verifies the result. 
   * @throws Exception
   */
  public void testPRRRLocalQuerying() throws Exception
  {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, Portfolio.class));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloName, NewPortfolio.class));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName, NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = PRQHelp.createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = PRQHelp.createNewPortfoliosAndPositions(cntDest);
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));
    
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio,
        cnt, cntDest));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");
    
    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRColocatedDataSetQueryAndCompareResults(name, coloName, localName, coloLocalName));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  /**
   * A very basic dunit test that <br>
   * 1. Creates two PR Data Stores with redundantCopies = 1.
   * 2. Populates the region with test data.
   * 3. Fires a LOCAL query on one data store VM and verifies the result. 
   * @throws Exception
   */
  public void testPRRRLocalQueryingWithIndexes() throws Exception {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, Portfolio.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex1", "r1.ID", "/"+name+" r1", null));
    //vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex11", "r1.status", "/"+name+" r1", null));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloName, NewPortfolio.class));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex2", "r2.id", "/"+coloName+" r2", null));
    //vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex22", "r2.status", "/"+coloName+" r2", null));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName, NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = PRQHelp.createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = PRQHelp.createNewPortfoliosAndPositions(cntDest);
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));
    
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio,
        cnt, cntDest));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");
    
    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRColocatedDataSetQueryAndCompareResults(name, coloName, localName, coloLocalName));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  /**
   * A very basic dunit test that <br>
   * 1. Creates two PR Data Stores with redundantCopies = 1.
   * 2. Populates the region with test data.
   * 3. Fires a LOCAL query on one data store VM and verifies the result. 
   * @throws Exception
   */
  public void testPRRRLocalQueryingWithIndexOnOnePRRegion() throws Exception {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, Portfolio.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex1", "r1.ID", "/"+name+" r1", null));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex11", "r1.status", "/"+name+" r1", null));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloName, NewPortfolio.class));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName, NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = PRQHelp.createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = PRQHelp.createNewPortfoliosAndPositions(cntDest);
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));
    
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio,
        cnt, cntDest));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");
    
    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRColocatedDataSetQueryAndCompareResults(name, coloName, localName, coloLocalName));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  /**
   * A very basic dunit test that <br>
   * 1. Creates two PR Data Stores with redundantCopies = 1.
   * 2. Populates the region with test data.
   * 3. Fires a LOCAL query on one data store VM and verifies the result. 
   * @throws Exception
   */
  public void testRRPRLocalQuerying() throws Exception
  {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(coloName,
        redundancy, NewPortfolio.class));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(name, Portfolio.class));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName, NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = PRQHelp.createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = PRQHelp.createNewPortfoliosAndPositions(cntDest);
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));
    
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio,
        cnt, cntDest));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");
    
    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRColocatedDataSetQueryAndCompareResults(name, coloName, localName, coloLocalName));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  /**
   * A very basic dunit test that <br>
   * 1. Creates two PR Data Stores with redundantCopies = 1.
   * 2. Populates the region with test data.
   * 3. Fires a LOCAL query on one data store VM and verifies the result. 
   * @throws Exception
   */
  public void testRRPRLocalQueryingWithIndexes() throws Exception {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(coloName,
        redundancy, NewPortfolio.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex1", "r2.id", "/"+coloName+" r2", null));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(name, Portfolio.class));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex2", "r1.ID", "/"+name+" r1", null));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName, NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = PRQHelp.createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = PRQHelp.createNewPortfoliosAndPositions(cntDest);
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));
    
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio,
        cnt, cntDest));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");
    
    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRColocatedDataSetQueryAndCompareResults(name, coloName, localName, coloLocalName));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  /**
   * A very basic dunit test that <br>
   * 1. Creates two PR Data Stores with redundantCopies = 1.
   * 2. Populates the region with test data.
   * 3. Fires a LOCAL query on one data store VM and verifies the result. 
   * @throws Exception
   */
  public void testRRPRLocalQueryingWithIndexOnOnePRRegion() throws Exception {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(coloName,
        redundancy, NewPortfolio.class));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(name, Portfolio.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex1", "r1.ID", "/"+name+" r1", null));

    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName, NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = PRQHelp.createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = PRQHelp.createNewPortfoliosAndPositions(cntDest);
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));
    
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio,
        cnt, cntDest));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");
    
    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRColocatedDataSetQueryAndCompareResults(name, coloName, localName, coloLocalName));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  /**
   * A very basic dunit test that <br>
   * 1. Creates two PR Data Stores with redundantCopies = 1.
   * 2. Populates the region with test data.
   * 3. Fires a LOCAL query on one data store VM and verifies the result. 
   * @throws Exception
   */
  public void testPRNonLocalQueryException() throws Exception {
    
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, Portfolio.class));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, Portfolio.class));
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRColocatedCreate(coloName,
        redundancy, name));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRColocatedCreate(coloName,
        redundancy, name));
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName, NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = PRQHelp.createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = PRQHelp.createNewPortfoliosAndPositions(cntDest);
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));
    
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio,
        cnt, cntDest));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");
    
    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(new CacheSerializableRunnable("PRQuery") {
      @Override
      public void run2() throws CacheException {

        Cache cache = getCache();
        // Querying the PR region
        
        String[] queries = new String[] {
            "r1.ID = r2.id",
            };

        Object r[][] = new Object[queries.length][2];
        Region region = null;
        region = cache.getRegion(name);
        assertNotNull(region);
        region = cache.getRegion(coloName);
        assertNotNull(region);
        region = cache.getRegion(localName);
        assertNotNull(region);
        region = cache.getRegion(coloLocalName);
        assertNotNull(region);

        final String[] expectedExceptions = new String[] {
            RegionDestroyedException.class.getName(),
            ReplyException.class.getName(),
            CacheClosedException.class.getName(),
            ForceReattemptException.class.getName(),
            QueryInvocationTargetException.class.getName() };

        for (int i = 0; i < expectedExceptions.length; i++) {
          getCache().getLogger().info(
              "<ExpectedException action=add>" + expectedExceptions[i]
                  + "</ExpectedException>");
        }

        QueryService qs = getCache().getQueryService();
        Object[] params;
        try {
          for (int j = 0; j < queries.length; j++) {
            getCache().getLogger().info(
                "About to execute local query: " + queries[j]);
            r[j][1] = qs.newQuery(
                "Select " + (queries[j].contains("ORDER BY") ? "DISTINCT" : "")
                    + " * from /" + name + " r1, /" + coloName + " r2 where "
                    + queries[j]).execute();
          }
          fail("PRQueryDUnitHelper#getCacheSerializableRunnableForPRQueryAndCompareResults: Queries Executed successfully on Local region & PR Region");

        } catch (QueryInvocationTargetException e) {
          // throw an unchecked exception so the controller can examine the
          // cause and see whether or not it's okay
          throw new TestException(
              "PRQueryDUnitHelper#getCacheSerializableRunnableForPRQueryAndCompareResults: Caught unexpected query exception",
              e);
        } catch (QueryException e) {
          getLogWriter()
              .error(
                  "PRQueryDUnitHelper#getCacheSerializableRunnableForPRQueryAndCompareResults: Caught QueryException while querying"
                      + e, e);
          throw new TestException(
              "PRQueryDUnitHelper#getCacheSerializableRunnableForPRQueryAndCompareResults: Caught unexpected query exception",
              e);
        } catch (UnsupportedOperationException uso) {
          getLogWriter().info(uso.getMessage());
          if (!uso.getMessage().equalsIgnoreCase(LocalizedStrings.DefaultQuery_A_QUERY_ON_A_PARTITIONED_REGION_0_MAY_NOT_REFERENCE_ANY_OTHER_REGION_1.toLocalizedString(new Object[] {name, "/"+coloName}))) {
            fail("Query did not throw UnsupportedOperationException while using QueryService instead of LocalQueryService");
          } else {
            getLogWriter().info("Query received UnsupportedOperationException successfully while using QueryService.");
          }
        } finally {
          for (int i = 0; i < expectedExceptions.length; i++) {
            getCache().getLogger().info(
                "<ExpectedException action=remove>" + expectedExceptions[i]
                    + "</ExpectedException>");
          }
        }
      }
    });

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  public void testPRRRLocalQueryingWithHetroIndexes() throws Exception {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, Portfolio.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex1", "r1.ID", "/"+name+" r1", null));
    //vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex11", "r1.status", "/"+name+" r1", null));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloName, NewPortfolio.class));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex2", "r2.id", "/"+coloName+" r2, r2.positions.values pos2", null));
    //vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex22", "r2.status", "/"+coloName+" r2", null));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName, NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = PRQHelp.createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = PRQHelp.createNewPortfoliosAndPositions(cntDest);
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));
    
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio,
        cnt, cntDest));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");
    
    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAndRRQueryAndCompareResults(name, coloName, localName, coloLocalName));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }


  public void testRRPRLocalQueryingWithHetroIndexes() throws Exception {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(coloName,
        redundancy, NewPortfolio.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex1", "r2.id", "/"+coloName+" r2", null));
    //vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex11", "r1.status", "/"+name+" r1", null));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(name, Portfolio.class));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex2", "r1.ID", "/"+name+" r1, r1.positions.values pos1", null));
    //vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex22", "r2.status", "/"+coloName+" r2", null));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName, NewPortfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = PRQHelp.createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = PRQHelp.createNewPortfoliosAndPositions(cntDest);
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));
    
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio,
        cnt, cntDest));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");
    
    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForRRAndPRQueryAndCompareResults(name, coloName, localName, coloLocalName));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  public void testPRRRCompactRangeAndNestedRangeIndexQuerying() throws Exception {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, Portfolio.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex1", "r1.ID", "/"+name+" r1", null));
    //vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex11", "r1.status", "/"+name+" r1", null));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloName, Portfolio.class));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex2", "pos2.id", "/"+coloName+" r2, r2.positions.values pos2", null));
    //vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex22", "r2.status", "/"+coloName+" r2", null));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName, Portfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = PRQHelp.createPortfoliosAndPositions(cntDest);
    final Portfolio[] newPortfolio = PRQHelp.createPortfoliosAndPositions(cntDest);
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));
    
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio,
        cnt, cntDest));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");
    
    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAndRRQueryWithCompactAndRangeIndexAndCompareResults(name, coloName, localName, coloLocalName));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  public void testPRRRIndexQueryWithSameTypeIndexQueryResults() throws Exception {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, Portfolio.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex1", "r1.ID", "/"+name+" r1", null));
    //vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex11", "r1.status", "/"+name+" r1", null));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionWithAsyncIndexCreation(coloName, NewPortfolio.class));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex2", "r2.id", "/"+coloName+" r2", null));
    //vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex22", "r2.status", "/"+coloName+" r2", null));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Colocated DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name, "IdIndex3", "r1.ID", "/"+localName+" r1", null));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloLocalName, NewPortfolio.class));

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(coloName, "IdIndex4", "r2.id", "/"+coloLocalName+" r2", null));
    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = PRQHelp.createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = PRQHelp.createNewPortfoliosAndPositions(cntDest);
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloLocalName, newPortfolio,
        cnt, cntDest));
    
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio,
        cnt, cntDest));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");

    //Let async index updates be finished.
    pause(5000);

    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAndRRQueryAndCompareResults(name, coloName, localName, coloLocalName));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  /**
   * A very basic dunit test that <br>
   * 1. Creates two PR Data Stores with redundantCopies = 1.
   * 2. Populates the region with test data.
   * 3. Fires a LOCAL query on one data store VM and verifies the result. 
   * @throws Exception
   */
  public void testPRRRNonLocalQueryingWithNoRROnOneNode() throws Exception
  {
    
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the DataStore node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        0, Portfolio.class));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        0, Portfolio.class));
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the DataStore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Creating Colocated Region DataStore node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Colocated DataStore node in the RR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(coloName, NewPortfolio.class));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    final Portfolio[] portfolio = PRQHelp.createPortfoliosAndPositions(cntDest);
    final NewPortfolio[] newPortfolio = PRQHelp.createNewPortfoliosAndPositions(cntDest);
    
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(coloName, newPortfolio,
        cnt, cntDest));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");
    
    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(new CacheSerializableRunnable("PRQuery") {
      @Override
      public void run2() throws CacheException {

     // Helper classes and function
        final class TestQueryFunction extends FunctionAdapter {

          @Override
          public boolean hasResult() {
            return true;
          }

          @Override
          public boolean isHA() {
            return false;
          }

          private final String id;

          public TestQueryFunction(String id) {
            super();
            this.id = id;
          }

          @Override
          public void execute(FunctionContext context) {
            Cache cache = CacheFactory.getAnyInstance();
            QueryService queryService = cache.getQueryService();
            ArrayList allQueryResults = new ArrayList();
            String qstr = (String) context.getArguments();
            try {
              Query query = queryService.newQuery(qstr);
              context.getResultSender().sendResult((ArrayList) ((SelectResults) query
                      .execute((RegionFunctionContext) context)).asList());
              context.getResultSender().lastResult(null);
            } catch (Exception e) {
              e.printStackTrace();
              throw new FunctionException(e);
            }
          }

          @Override
          public String getId() {
            return this.id;
          }
        }
        Cache cache = getCache();
        // Querying the PR region
        
        String[] queries = new String[] {
            "r1.ID = r2.id",
            };

        Object r[][] = new Object[queries.length][2];
        Region region = null;
        region = cache.getRegion(name);
        assertNotNull(region);
        region = cache.getRegion(coloName);
        assertNotNull(region);
        
        QueryService qs = getCache().getQueryService();
        Object[] params;
        try {
          for (int j = 0; j < queries.length; j++) {
            getCache().getLogger().info(
                "About to execute local query: " + queries[j]);
            Function func = new TestQueryFunction("testfunction");
            
            Object funcResult = FunctionService.onRegion((getCache().getRegion(name) instanceof PartitionedRegion)? getCache().getRegion(name) : getCache().getRegion(coloName)).withArgs(
                "Select " + (queries[j].contains("ORDER BY") ? "DISTINCT" : "")
                    + " * from /" + name + " r1, /" + coloName
                    + " r2 where " + queries[j]).execute(func).getResult();

            r[j][0] = ((ArrayList)funcResult).get(0);
          }
          fail("PRQueryDUnitHelper#getCacheSerializableRunnableForPRQueryAndCompareResults: Queries Executed successfully without RR region on one of the nodes");

        } catch (FunctionException e) {
          if (e.getCause() instanceof RegionNotFoundException) {
            getLogWriter().info("Query received FunctionException successfully while using QueryService.");
          } else {
            fail("RegionNotFoundException must be thrown here");
          }
        }
      }
    });

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }
}
