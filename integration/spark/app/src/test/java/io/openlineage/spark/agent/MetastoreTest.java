package io.openlineage.spark.agent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
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
  @Disabled
  @Test
  void IcebergTablesTest() {
    SparkSession spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("IcebergMetastoreTest")
            .config("spark.driver.host", LOCAL_IP)
            .config("org.jpox.autoCreateSchema", "true")
            .config("javax.jdo.option.ConnectionURL", String.format("jdbc:postgresql://localhost:%d/metastore31", mappedPort))
            .config("javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver")
            .config("javax.jdo.option.ConnectionUserName", "hiveuser")
            .config("javax.jdo.option.ConnectionPassword", "password")
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config("spark.sql.warehouse.dir", "gs://gidasttn-dev-bucket/warehouse")
            .config("spark.hadoop.google.cloud.project.id", "gidasttn-dev")
            .config("spark.hadoop.google.cloud.auth.service.account.enable","true")
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile","/home/tomasznazarewicz/Downloads/gidasttn-dev-f70fa4f71525.json")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/col_v2/derby")
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
    SparkSession spark =
            SparkSession.builder()
                    .master("local[*]")
                    .appName("NonIcebergMetastoreTest")
                    .config("spark.driver.host", LOCAL_IP)
                    .config("org.jpox.autoCreateSchema", "true")
                    .config("javax.jdo.option.ConnectionURL", String.format("jdbc:postgresql://localhost:%d/metastore23", mappedPort))
                    .config("javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver")
                    .config("javax.jdo.option.ConnectionUserName", "hiveuser")
                    .config("javax.jdo.option.ConnectionPassword", "password")
                    .config("spark.sql.warehouse.dir", "gs://gidasttn-dev-bucket/warehouse")
                    .config("spark.hadoop.google.cloud.project.id", "gidasttn-dev")
                    .config("spark.hadoop.google.cloud.auth.service.account.enable","true")
                    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile","/home/tomasznazarewicz/Downloads/gidasttn-dev-f70fa4f71525.json")
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
