package io.openlineage.spark.agent;

import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;

@Slf4j
public class MetastoreTest {
  private static final String LOCAL_IP = "127.0.0.1";
  private static final int ORIGINAL_PORT = 5432;
  private static Network network = Network.newNetwork();
  @Container
  private static PostgreSQLContainer metastoreContainer = SparkContainerUtils.makeMetastoreContainer(network);
  private static int mappedPort;
  
  @BeforeAll
  public static void setup(){
    metastoreContainer.start();
    mappedPort = metastoreContainer.getMappedPort(ORIGINAL_PORT);
  }
  
  @AfterAll
  public static void tearDown(){
    metastoreContainer.stop();
  }

  private static SparkConf getCommonSparkConf(String appName, String metastore) {
    return new SparkConf()
            .setAppName(appName)
            .setMaster("local[*]")
            .set("spark.driver.host", LOCAL_IP)
            .set("org.jpox.autoCreateSchema", "true")
            .set("javax.jdo.option.ConnectionURL", String.format("jdbc:postgresql://localhost:%d/%s", mappedPort, metastore))
            .set("javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver")
            .set("javax.jdo.option.ConnectionUserName", "hiveuser")
            .set("javax.jdo.option.ConnectionPassword", "password")
            .set("spark.sql.warehouse.dir", "gs://gidasttn-dev-bucket/warehouse")
            .set("spark.hadoop.google.cloud.project.id", "gidasttn-dev")
            .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
            .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/home/tomasznazarewicz/Downloads/gidasttn-dev-f70fa4f71525.json");
  }
  @Disabled
  @Test
  void IcebergTablesTest() {
    SparkSession spark =
        SparkSession.builder()
            .config(getCommonSparkConf("IcebergMetastoreTest", "metastore31"))
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .enableHiveSupport()
            .getOrCreate();
    
    HiveCatalog catalog = new HiveCatalog();
    catalog.setConf(spark.sparkContext().hadoopConfiguration());
    spark.sql("create database if not exists iceberg_test");
    spark.sql("use iceberg_test");
    spark.sql("drop table if exists iceberg_test.test_table");
    spark.sql("create external table iceberg_test.test_table (id int, value string) USING iceberg location 'gs://gidasttn-dev-bucket/warehouse/iceberg_test.db/test_table'");
    spark.sql("show schemas");
    spark.sql("show tables");
    spark.sql("select * from iceberg_test.test_table").show();
    spark.close();

    List<TableIdentifier> tables = catalog.listTables(Namespace.of("iceberg_test"));
    Table table1 = catalog.loadTable(tables.get(0));
  }

  @Disabled
  @Test
  void NonIcebergTablesTest() {
    SparkSession spark = SparkSession.builder()
      .config(getCommonSparkConf("NonIcebergMetastoreTest", "metastore23"))
      .enableHiveSupport()
      .getOrCreate();
    
    spark.sql("create database if not exists no_iceberg_test");
    spark.sql("drop table if exists test_table");
    spark.sql("create external table no_iceberg_test.test_table (id int, value string) location 'gs://gidasttn-dev-bucket/warehouse/no_iceberg_test.db/test_table'");
    spark.sql("show schemas");
    spark.sql("show tables");
    spark.sql("select * from no_iceberg_test.test_table").show();
    spark.close();
  }
}
