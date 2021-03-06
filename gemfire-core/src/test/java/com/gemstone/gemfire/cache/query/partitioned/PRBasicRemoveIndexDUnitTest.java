/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.partitioned;

import com.gemstone.gemfire.cache.query.data.PortfolioData;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDUnitTestCase;

import dunit.Host;
import dunit.VM;

/**
 * Basic funtional test for removing index from a partitioned region system.
 * @author rdubey
 * 
 */
public class PRBasicRemoveIndexDUnitTest extends PartitionedRegionDUnitTestCase
{
  /**
   * Constructor
   * @param name
   */  
  public PRBasicRemoveIndexDUnitTest (String name) {
    super(name);
  }
  
  PRQueryDUnitHelper PRQHelp = new PRQueryDUnitHelper("");
  
  /**
   * Name of the partitioned region for the test.
   */
  final String name = "PartionedPortfolios";
  
  
  final int start = 0;
  
  final int end = 1003;

  /**
   * Reduncancy level for the pr.
   */
  final int redundancy = 0;

  
  /**
   * Remove index test to remove all the indexes in a given partitioned region
   * 
   * @throws Exception
   *           if the test fails
   */
  public void testPRBasicIndexRemove() throws Exception
  {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    
    getLogWriter().info(
        "PRBasicRemoveIndexDUnitTest.testPRBasicIndexCreate test now starts ....");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    
    final PortfolioData[] portfolio = PRQHelp.createPortfolioData(start, end);
    // Putting the data into the PR's created
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        start, end));
    
    // create all the indexes.
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnPKID", "p.pkid",null, "p"));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnStatus", "p.status",null, "p"));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnId", "p.ID",null, "p"));
    
    //remove indexes
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForRemoveIndex(name, false));
    
    getLogWriter().info(
    "PRBasicRemoveIndexDUnitTest.testPRBasicRemoveIndex test now  ends sucessfully");

  }
  
  /**
   * Test removing single index on a pr.
   */
  public void testPRBasicRemoveParticularIndex() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    
    getLogWriter().info(
        "PRBasicRemoveIndexDUnitTest.testPRBasicIndexCreate test now starts ....");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    final PortfolioData[] portfolio = PRQHelp.createPortfolioData(start, end);
    // Putting the data into the PR's created
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        start, end));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnPKID", "p.pkid",null, "p"));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnStatus", "p.status",null, "p"));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnId", "p.ID",null, "p"));
    
//  remove indexes
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForRemoveIndex(name, true));
    
    
  }
  
}
