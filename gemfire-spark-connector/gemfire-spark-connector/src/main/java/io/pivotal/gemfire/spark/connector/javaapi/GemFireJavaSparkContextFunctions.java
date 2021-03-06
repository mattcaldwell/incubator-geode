package io.pivotal.gemfire.spark.connector.javaapi;


import io.pivotal.gemfire.spark.connector.GemFireConnectionConf;
import io.pivotal.gemfire.spark.connector.internal.rdd.GemFireRegionRDD;
import io.pivotal.gemfire.spark.connector.internal.rdd.GemFireRegionRDD$;
import org.apache.spark.SparkContext;
import static io.pivotal.gemfire.spark.connector.javaapi.JavaAPIHelper.*;

import scala.reflect.ClassTag;
import java.util.Properties;

/**
 * Java API wrapper over {@link org.apache.spark.SparkContext} to provide GemFire
 * Connector functionality.
 *
 * <p></p>To obtain an instance of this wrapper, use one of the factory methods in {@link
 * io.pivotal.gemfire.spark.connector.javaapi.GemFireJavaUtil} class.</p>
 */
public class GemFireJavaSparkContextFunctions {

  public final SparkContext sc;

  public GemFireJavaSparkContextFunctions(SparkContext sc) {
    this.sc = sc;
  }

  /**
   * Expose a GemFire region as a JavaPairRDD
   * @param regionPath the full path of the region
   * @param connConf the GemFireConnectionConf that can be used to access the region
   * @param opConf the parameters for this operation, such as preferred partitioner.
   */
  public <K, V> GemFireJavaRegionRDD<K, V> gemfireRegion(
    String regionPath, GemFireConnectionConf connConf, Properties opConf) {
    ClassTag<K> kt = fakeClassTag();
    ClassTag<V> vt = fakeClassTag();    
    GemFireRegionRDD<K, V>  rdd =  GemFireRegionRDD$.MODULE$.apply(
      sc, regionPath, connConf, propertiesToScalaMap(opConf), kt, vt);
    return new GemFireJavaRegionRDD<>(rdd);
  }

  /**
   * Expose a GemFire region as a JavaPairRDD with default GemFireConnector and no preferred partitioner.
   * @param regionPath the full path of the region
   */
  public <K, V> GemFireJavaRegionRDD<K, V> gemfireRegion(String regionPath) {
    GemFireConnectionConf connConf = GemFireConnectionConf.apply(sc.getConf());
    return gemfireRegion(regionPath, connConf, new Properties());
  }

  /**
   * Expose a GemFire region as a JavaPairRDD with no preferred partitioner.
   * @param regionPath the full path of the region
   * @param connConf the GemFireConnectionConf that can be used to access the region
   */
  public <K, V> GemFireJavaRegionRDD<K, V> gemfireRegion(String regionPath, GemFireConnectionConf connConf) {
    return gemfireRegion(regionPath, connConf, new Properties());
  }

  /**
   * Expose a GemFire region as a JavaPairRDD with default GemFireConnector.
   * @param regionPath the full path of the region
   * @param opConf the parameters for this operation, such as preferred partitioner.
   */
  public <K, V> GemFireJavaRegionRDD<K, V> gemfireRegion(String regionPath, Properties opConf) {
    GemFireConnectionConf connConf = GemFireConnectionConf.apply(sc.getConf());
    return gemfireRegion(regionPath, connConf, opConf);
  }

}
