/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/** Class containing pdx tests.
 * 
 */
package com.gemstone.gemfire.pdx;

import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.SerializationException;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.util.test.TestUtil;

import dunit.*;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

public class VersionClassLoader {

  /** If PdxPrms.initClassLoader is true, then randomly choose a versioned
   *  class path and create and install a class loader for it on this thread.
   *  
   * @return The installed class loader (which includes a versioned class path)
   *         or null if this call did not install a new class loader.
   */
  public static ClassLoader initClassLoader(long classVersion) throws Exception {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    cl = ClassLoader.getSystemClassLoader();
    
    String alternateVersionClassPath =  System.getProperty("JTESTS") +
    File.separator + ".." + File.separator + ".." + File.separator +
    "classes" + File.separator + "version" + classVersion;
    DistributedTestCase.getLogWriter().info("Initializing the class loader :" + alternateVersionClassPath);
    ClassLoader versionCL = null;
    try {
      versionCL = new URLClassLoader(new URL[]{new File(alternateVersionClassPath).toURI().toURL()}, cl);
      Thread.currentThread().setContextClassLoader(versionCL); 
    } catch (Exception e) {
      DistributedTestCase.getLogWriter().info("error", e);
      throw new Exception("Failed to initialize the class loader. " + e.getMessage());
    }
    DistributedTestCase.getLogWriter().info("Setting/adding class loader with " + alternateVersionClassPath);
    return versionCL;
  }


  /** Use reflection to create a new instance of a versioned class whose
   *  name is specified by className.
   *  
   *  Since versioned classes are compled outside outside the <checkoutDir>/tests 
   *  directory, code within <checkoutDir>/tests cannot directly reference
   *  versioned classes, however the versioned class should be available at 
   *  runtime if the test has installed the correct class loader.
   *
   * @param className The name of the versioned class to create. 
   * @return A new instance of className.
   */
  public static Object getVersionedInstance(String className, Object[] args) throws Exception {
    Object newObj = null;
    try {
      Class aClass = Class.forName(className, true, Thread.currentThread().getContextClassLoader());
      if (args != null && args.length > 0) {
        if (className.endsWith("PdxTestObject")){
          Constructor constructor = aClass.getConstructor(int.class, String.class);
          newObj = constructor.newInstance(((Integer)args[0]).intValue(), args[1]);          
        } else if (className.endsWith("PortfolioPdxVersion1")) {
          Constructor constructor = aClass.getConstructor(int.class, int.class);
          newObj = constructor.newInstance(((Integer)args[0]).intValue(), ((Integer)args[1]).intValue());
        } else if (className.endsWith("PdxVersionedNewPortfolio")) {
          Constructor constructor = aClass.getConstructor(String.class, int.class);
          newObj = constructor.newInstance(((String)args[0]), ((Integer)args[1]).intValue());
        } else if (className.endsWith("PdxVersionedFieldType")) {
          Constructor constructor = aClass.getConstructor( int.class);
          newObj = constructor.newInstance(((Integer)args[0]).intValue());
        } 
      } else {
        Constructor constructor = aClass.getConstructor();
        newObj = constructor.newInstance();
      }
    } catch (Exception e) {
      DistributedTestCase.getLogWriter().info("error", e);
      throw new Exception("Failed to get the class instance. ClassName: " + className + "  error: ", e);
    }
    return newObj;
  }
}
