package com.gemstone.gemfire.internal.logging;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import static org.junit.Assert.*;
import junit.framework.TestCase;

import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.logging.log4j.LogWriterLogger;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

import dunit.DistributedTestCase;
import dunit.DistributedTestCase.WaitCriterion;

/**
 * Creates Locator and tests logging behavior at a high level.
 * 
 * @author Kirk Lund
 */
@Category(IntegrationTest.class)
public class LocatorLogFileJUnitTest {
  @Rule public TestName name = new TestName();

  protected static final int TIMEOUT_MILLISECONDS = 180 * 1000; // 2 minutes
  protected static final int INTERVAL_MILLISECONDS = 100; // 100 milliseconds
  
  private Locator locator;
  private FileInputStream fis;
  
  
  @After
  public void tearDown() throws Exception {
    if (this.locator != null) {
      this.locator.stop();
      this.locator = null;
    }
    if (fis != null) {
      fis.close();
    }
  }
  
  @Test
  public void testLocatorCreatesLogFile() throws Exception {
    final int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String locators = "localhost[" + port + "]";

    final Properties properties = new Properties();
    properties.put("log-level", "config");
    properties.put("mcast-port", "0");
    properties.put("locators", locators);
    properties.put("enable-network-partition-detection", "false");
    properties.put("disable-auto-reconnect", "true");
    properties.put("member-timeout", "2000");
    properties.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    
    final File logFile = new File(name.getMethodName() + "-locator-" + port + ".log");
    if (logFile.exists()) {
      logFile.delete();
    }
    assertFalse(logFile.exists());
    
    this.locator = Locator.startLocatorAndDS(port, logFile, properties);
    
    InternalDistributedSystem ds = (InternalDistributedSystem)this.locator.getDistributedSystem();
    assertNotNull(ds);
    DistributionConfig config = ds.getConfig();
    assertNotNull(config);
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.CONFIG_LEVEL) + " but was " + LogWriterImpl.levelToString(config.getLogLevel()),
        InternalLogWriter.CONFIG_LEVEL, config.getLogLevel());
    
    // CONFIG has been replaced with INFO -- all CONFIG statements are now logged at INFO as well
    InternalLogWriter logWriter = (InternalLogWriter) ds.getLogWriter();
    assertNotNull(logWriter);
    assertTrue(logWriter instanceof LogWriterLogger);
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.INFO_LEVEL) + " but was " + LogWriterImpl.levelToString(logWriter.getLogWriterLevel()),
        InternalLogWriter.INFO_LEVEL, logWriter.getLogWriterLevel());
    
    assertNotNull(this.locator);
    DistributedTestCase.waitForCriterion(new WaitCriterion() {
      @Override
      public boolean done() {
        return logFile.exists();
      }
      @Override
      public String description() {
        return "waiting for log file to exist: " + logFile;
      }
    }, TIMEOUT_MILLISECONDS, INTERVAL_MILLISECONDS, true);
    assertTrue(logFile.exists());
    // assert not empty
    this.fis = new FileInputStream(logFile);
    assertTrue(fis.available() > 0);
    this.locator.stop();
    this.locator = null;
    this.fis.close();
    this.fis = null;
  }
}