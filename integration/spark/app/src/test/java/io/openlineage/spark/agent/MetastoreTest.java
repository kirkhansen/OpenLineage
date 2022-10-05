package io.openlineage.spark.agent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Tag("integration-test")
@Testcontainers
public class MetastoreTest {
  private static final String LOCAL_IP = "127.0.0.1";
  private static final int ORIGINAL_PORT = 5432;
  private static Network network = Network.newNetwork();

  @Container
  private static PostgreSQLContainer metastoreContainer =
      SparkContainerUtils.makeMetastoreContainer(network);

  private static int mappedPort;

  @BeforeAll
  public static void setup() {
    metastoreContainer.start();
    mappedPort = metastoreContainer.getMappedPort(ORIGINAL_PORT);
  }

  @AfterAll
  public static void tearDown() {
    metastoreContainer.stop();
  }

  private static SparkConf getCommonSparkConf(String appName, String metastore) {
    String json = System.getenv("GCLOUD_SERVICE_KEY");
    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, String> m;
    try {
      m = objectMapper.readValue(json, Map.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    return new SparkConf()
        .setAppName(appName)
        .setMaster("local[*]")
        .set("spark.driver.host", LOCAL_IP)
        .set("org.jpox.autoCreateSchema", "true")
        .set(
            "javax.jdo.option.ConnectionURL",
            String.format("jdbc:postgresql://localhost:%d/%s", mappedPort, metastore))
        .set("javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver")
        .set("javax.jdo.option.ConnectionUserName", "hiveuser")
        .set("javax.jdo.option.ConnectionPassword", "password")
        .set("spark.sql.warehouse.dir", "gs://gidasttn-dev-bucket/warehouse")
        .set("spark.hadoop.google.cloud.project.id", "gidasttn-dev")
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .set("fs.gs.auth.service.account.private.key.id", m.get("private_key_id"))
        .set("fs.gs.auth.service.account.private.key", m.get("private_key"))
        .set("fs.gs.auth.service.account.email", m.get("client_email"));
  }

  //  @Disabled
  //  @Test
  //  void IcebergTablesTest() {
  //    SparkSession spark =
  //        SparkSession.builder()
  //            .config(getCommonSparkConf("IcebergMetastoreTest", "metastore31"))
  //            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
  //            .config("spark.sql.catalog.spark_catalog.type", "hive")
  //            .config(
  //                "spark.sql.extensions",
  //                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  //            .enableHiveSupport()
  //            .getOrCreate();
  //
  //    HiveCatalog catalog = new HiveCatalog();
  //    catalog.setConf(spark.sparkContext().hadoopConfiguration());
  //    spark.sql("create database if not exists iceberg_test");
  //    spark.sql("use iceberg_test");
  //    spark.sql("drop table if exists iceberg_test.test_table");
  //    spark.sql(
  //        "create external table iceberg_test.test_table (id int, value string) USING iceberg
  // location 'gs://gidasttn-dev-bucket/warehouse/iceberg_test.db/test_table'");
  //    spark.sql("show schemas");
  //    spark.sql("show tables");
  //    spark.sql("select * from iceberg_test.test_table").show();
  //    spark.close();
  //
  //    List<TableIdentifier> tables = catalog.listTables(Namespace.of("iceberg_test"));
  //    Table table1 = catalog.loadTable(tables.get(0));
  //  }

  @Test
  void NonIcebergTablesTest() {
    SparkSession spark =
        SparkSession.builder()
            .config(getCommonSparkConf("NonIcebergMetastoreTest", "metastore23"))
            .enableHiveSupport()
            .getOrCreate();

    spark.sql("create database if not exists no_iceberg_test");
    spark.sql("drop table if exists test_table");
    spark.sql(
        "create external table no_iceberg_test.test_table (id int, value string) location 'gs://gidasttn-dev-bucket/warehouse/no_iceberg_test.db/test_table'");
    spark.sql("show schemas");
    spark.sql("show tables");
    spark.sql("select * from no_iceberg_test.test_table").show();
    spark.close();
  }
}
