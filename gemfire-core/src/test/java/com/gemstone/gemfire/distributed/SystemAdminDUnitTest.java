/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.SystemAdmin;

import dunit.DistributedTestCase;

public class SystemAdminDUnitTest extends DistributedTestCase {

  public SystemAdminDUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    disconnect();
  }
  
  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
    disconnect();
  }
  
  public void disconnect() {
    // get rid of the command-line distributed system created by SystemAdmin
    system = null;
    InternalDistributedSystem sys = InternalDistributedSystem.getAnyInstance();
    if (sys != null && sys.isConnected()) {
      getLogWriter().info("disconnecting(3)");
      sys.disconnect();
    }
  }

  public void testPrintStacks() throws Exception {

    // create a gemfire.properties that lets SystemAdmin find the dunit locator
    Properties p = getAllDistributedSystemProperties(getDistributedSystemProperties());
    try {
      
      SystemAdmin.setDistributedSystemProperties(p);
      
      String filename2 = getUniqueName()+"2.txt";
      List<String> options = new ArrayList<String>(1);
      options.add(filename2);
      SystemAdmin.printStacks(options, true);
      checkStackDumps(filename2, false);
  
      disconnect();
      
      String filename1 = getUniqueName()+"1.txt";
      options.clear();
      options.add(filename1);
      SystemAdmin.printStacks(options, false);
      checkStackDumps(filename1, true);
      
    } finally {
      // SystemAdmin calls methods that set these static variables
      DistributionManager.isDedicatedAdminVM = false;
      DistributionManager.isCommandLineAdminVM = false;
      SystemAdmin.setDistributedSystemProperties(null);
    }
  }

  private void checkStackDumps(String filename, boolean isPruned) throws IOException {
    File file = new File(filename);
    if (!file.exists()) {
      fail("printStacks did not create a stack dump");
    }
    BufferedReader in = new BufferedReader(new FileReader(file));
    // look for some threads that shouldn't be there
    boolean setting = !isPruned;
    boolean foundManagementTask = setting;
    boolean foundGCThread = setting;
    boolean foundFunctionThread = setting;
    String line;
    do {
      line = in.readLine();
      if (line != null) {
        if (line.contains("GemFire Garbage Collection")) foundGCThread = true;
        else if (line.contains("Management Task")) foundManagementTask = true;
        else if (line.contains("Function Execution Processor")) foundFunctionThread = true;
      }
    } while (line != null);

    if (isPruned) {
      assertFalse("found a GemFire Garbage Collection thread in stack dump in "+filename, foundGCThread);
      assertFalse("found a Management Task thread in stack dump in " + filename, foundManagementTask);
      assertFalse("found a Function Excecution Processor thread in stack dump in "+filename, foundFunctionThread);
    } else {
      assertTrue("found no GemFire Garbage Collection thread in stack dump in "+filename, foundGCThread);
      assertTrue("found no Management Task thread in stack dump in " + filename, foundManagementTask);
      assertTrue("found no Function Excecution Processor thread in stack dump in "+filename, foundFunctionThread);
   }
   file.delete();
  }
}
