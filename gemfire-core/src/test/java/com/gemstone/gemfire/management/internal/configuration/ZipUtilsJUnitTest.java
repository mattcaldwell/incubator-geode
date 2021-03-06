/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.configuration;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.management.internal.configuration.utils.ZipUtils;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * JUnit Test for {@link ZipUtils}
 * 
 * TODO: this is broken on Windows: see bug #52036
 * 
 * @author bansods
 */
@Category(UnitTest.class)
public class ZipUtilsJUnitTest {
  
  private static final String sourceFolderName = "sourceFolder";
  private static final String zipFileName = "target.zip";
  private static final String destinationFolderName = "destination";
  private static final String clusterFolderName = "cluster";
  private static final String groupFolderName = "group";
  private static final String clusterTextFileName = "cf.txt";
  private static final String groupTextFileName = "gf.txt";
  private static final String clusterText = "cluster content";
  private static final String groupText = "group content";
  
  @After
  public void tearDown() throws Exception {
    forceDelete(new File(sourceFolderName));
    forceDelete(new File(zipFileName));
    forceDelete(new File(destinationFolderName));
  }
  
  private void forceDelete(File f) throws Exception {
    try {
      FileUtils.forceDelete(f);
    } catch (FileNotFoundException e) {
      // ignored
    }
  }

  @Test
  public void testZipUtils() throws Exception {
    File sf = new File(sourceFolderName);
    File cf = new File(FilenameUtils.concat(sourceFolderName, clusterFolderName));
    File gf = new File(FilenameUtils.concat(sourceFolderName, groupFolderName));
    sf.mkdir();
    cf.mkdir();
    gf.mkdir();
    FileUtils.writeStringToFile(new File(FilenameUtils.concat(cf.getCanonicalPath(), clusterTextFileName)), clusterText);
    FileUtils.writeStringToFile(new File(FilenameUtils.concat(gf.getCanonicalPath(), groupTextFileName)), groupText);
    ZipUtils.zip(sourceFolderName, zipFileName);
    File zipFile = new File(zipFileName);
    assertTrue(zipFile.exists());
    assertTrue(zipFile.isFile());
    ZipUtils.unzip(zipFileName, destinationFolderName);
    
    File df = new File(destinationFolderName);
    assertTrue(df.exists());
    assertTrue(df.isDirectory());
    
    File[] subDirs = df.listFiles();
    assertTrue((subDirs != null) && (subDirs.length != 0));
    File dfClusterTextFile = new File(FilenameUtils.concat(destinationFolderName, clusterFolderName + File.separator + clusterTextFileName));
    File dfGroupTextFile = new File (FilenameUtils.concat(destinationFolderName, groupFolderName + File.separator + groupTextFileName));
    
    assertTrue(clusterText.equals(FileUtils.readFileToString(dfClusterTextFile)));
    assertTrue(groupText.equals(FileUtils.readFileToString(dfGroupTextFile)));
  }
}
