/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.commands;

import java.io.File;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.util.test.TestUtil;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * 
 * @author rishim
 * @since 8.1
 */
@Category(IntegrationTest.class)
public class HTTPServiceSSLSupportJUnitTest {

  private File jks;


  @Before
  public void setUp() throws Exception {
    jks = findTrustedJKS();
  }

  @After
  public void tearDown() throws Exception {
    System.clearProperty("gemfire.javax.net.ssl.keyStore");
    System.clearProperty("gemfire.javax.net.ssl.keyStorePassword");
    System.clearProperty("gemfire.javax.net.ssl.trustStore");
    System.clearProperty("gemfire.javax.net.ssl.trustStorePassword");
    System.clearProperty("gemfireSecurityPropertyFile");
  }

  private static File findTrustedJKS() {
    return new File(TestUtil.getResourcePath(HTTPServiceSSLSupportJUnitTest.class, "/ssl/trusted.keystore"));
  }

  public static String makePath(String[] strings) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < strings.length; i++) {
      sb.append(strings[i]);
      sb.append(File.separator);
    }
    return sb.toString();
  }

  public void _testSSLWithClusterSSL() throws Exception {

    Properties localProps = new Properties();
    localProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    localProps.setProperty(DistributionConfig.CLUSTER_SSL_ENABLED_NAME, "true");
    localProps.setProperty(DistributionConfig.CLUSTER_SSL_KEYSTORE_NAME, jks.getCanonicalPath());
    localProps.setProperty(DistributionConfig.CLUSTER_SSL_KEYSTORE_PASSWORD_NAME, "password");
    localProps.setProperty(DistributionConfig.CLUSTER_SSL_KEYSTORE_TYPE_NAME, "JKS");
    localProps.setProperty(DistributionConfig.CLUSTER_SSL_PROTOCOLS_NAME, "SSL");
    localProps.setProperty(DistributionConfig.CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME, "true");
    localProps.setProperty(DistributionConfig.CLUSTER_SSL_TRUSTSTORE_NAME, jks.getCanonicalPath());
    localProps.setProperty(DistributionConfig.CLUSTER_SSL_TRUSTSTORE_PASSWORD_NAME, "password");

    DistributionConfigImpl config = new DistributionConfigImpl(localProps);

    assertEquals(config.getHttpServiceSSLEnabled(), true);
    assertEquals(config.getHttpServiceSSLKeyStore(), jks.getCanonicalPath());
    assertEquals(config.getHttpServiceSSLKeyStorePassword(), "password");
    assertEquals(config.getHttpServiceSSLKeyStoreType(), "JKS");
    assertEquals(config.getHttpServiceSSLProtocols(), "SSL");
    assertEquals(config.getHttpServiceSSLRequireAuthentication(), true);
    assertEquals(config.getHttpServiceSSLTrustStore(), jks.getCanonicalPath());
    assertEquals(config.getHttpServiceSSLTrustStorePassword(), "password");

  }

  @Test
  public void testSSLWithDeprecatedClusterSSL_HTTPService() throws Exception {

    Properties localProps = new Properties();
    localProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    localProps.setProperty(DistributionConfig.SSL_ENABLED_NAME, "true");
    System.setProperty("gemfire.javax.net.ssl.keyStore", jks.getCanonicalPath());
    System.setProperty("gemfire.javax.net.ssl.keyStorePassword", "password");

    localProps.setProperty(DistributionConfig.SSL_PROTOCOLS_NAME, "SSL");
    localProps.setProperty(DistributionConfig.SSL_REQUIRE_AUTHENTICATION_NAME, "true");
    System.setProperty("gemfire.javax.net.ssl.trustStore", jks.getCanonicalPath());
    System.setProperty("gemfire.javax.net.ssl.trustStorePassword", "password");

    DistributionConfigImpl config = new DistributionConfigImpl(localProps);

    assertEquals(config.getHttpServiceSSLEnabled(), true);
    assertEquals(config.getHttpServiceSSLProtocols(), "SSL");
    assertEquals(config.getHttpServiceSSLRequireAuthentication(), true);

    assertEquals(config.getHttpServiceSSLProperties().get("javax.net.ssl.keyStore"), jks.getCanonicalPath());
    assertEquals(config.getHttpServiceSSLProperties().get("javax.net.ssl.keyStorePassword"), "password");
    // assertEquals(system.getConfig().getHttpServiceSSLKeyStoreType(),"JKS");
    assertEquals(config.getHttpServiceSSLProperties().get("javax.net.ssl.trustStore"), jks.getCanonicalPath());
    assertEquals(config.getHttpServiceSSLProperties().get("javax.net.ssl.trustStorePassword"), "password");

  }

  @Test
  public void testSSLWithDeprecatedClusterSSL_HTTPService_WithSSL_Properties() throws Exception {

    Properties localProps = new Properties();
    localProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    localProps.setProperty(DistributionConfig.SSL_ENABLED_NAME, "true");

    localProps.setProperty(DistributionConfig.SSL_PROTOCOLS_NAME, "SSL");
    localProps.setProperty(DistributionConfig.SSL_REQUIRE_AUTHENTICATION_NAME, "true");

    Properties sslProps = new Properties();
    sslProps.setProperty("javax.net.ssl.keyStore", jks.getCanonicalPath());
    sslProps.setProperty("javax.net.ssl.keyStorePassword", "password");
    sslProps.setProperty("javax.net.ssl.trustStore", jks.getCanonicalPath());
    sslProps.setProperty("javax.net.ssl.trustStorePassword", "password");

    localProps.putAll(sslProps);

    DistributionConfigImpl config = new DistributionConfigImpl(localProps);

    assertEquals(config.getHttpServiceSSLEnabled(), true);
    assertEquals(config.getHttpServiceSSLProtocols(), "SSL");
    assertEquals(config.getHttpServiceSSLRequireAuthentication(), true);

    assertEquals(config.getHttpServiceSSLProperties().get("javax.net.ssl.keyStore"), jks.getCanonicalPath());
    assertEquals(config.getHttpServiceSSLProperties().get("javax.net.ssl.keyStorePassword"), "password");
    assertEquals(config.getHttpServiceSSLProperties().get("javax.net.ssl.trustStore"), jks.getCanonicalPath());
    assertEquals(config.getHttpServiceSSLProperties().get("javax.net.ssl.trustStorePassword"), "password");

  }


}
