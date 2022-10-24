package io.openlineage.spark.agent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@Tag("integration-test")
@Testcontainers
public class MetastoreHive2Test {
  private static final String VERSION = System.getProperty("spark.version");
  private static final String database = "hive2";
  private static final String table = "test";
  private static final String BASE_PATH = "gs://gidasttn-dev-bucket/warehouse/" + VERSION + "/";
  //    private static final String BASE_PATH = "gs://openlineage-ci-testing/warehouse/" + VERSION + "/";
  private static Network network = Network.newNetwork();
  private static SparkSession spark;
  private static FileSystem fs;
  @Container
  private static PostgreSQLContainer metastoreContainer =
          SparkContainerUtils.makeMetastoreContainer(network);

  private static int mappedPort;

  @BeforeAll
  public static void setup() {
    metastoreContainer.start();
    mappedPort = metastoreContainer.getMappedPort(MetastoreTestUtils.POSTGRES_PORT);
    spark = SparkSession.builder()
            .config(MetastoreTestUtils.getCommonSparkConf("MetastoreHive2Test", "metastore23", metastoreContainer.getMappedPort(MetastoreTestUtils.POSTGRES_PORT), false))
            .enableHiveSupport()
            .getOrCreate();
    fs = MetastoreTestUtils.getFileSystem(spark);

    MetastoreTestUtils.removeBaseDir(BASE_PATH, fs);
//    MetastoreTestUtils.createSampleFiles(spark, BASE_PATH, database, table, true);
    executeSql("DROP TABLE IF EXISTS %s.%s", database, table);
    executeSql("DROP DATABASE IF EXISTS %s", database);
  }

  @AfterAll
  public static void tearDown() {
    metastoreContainer.stop();
    MetastoreTestUtils.removeBaseDir(BASE_PATH, fs);
    spark.close();

  }

  @Test
  void IcebergTablesTest() {
    executeSql("create database if not exists %s", database);
    executeSql("drop table if exists %s.%s", database, table);
    executeSql("create external table %s.%s (id int, value string) location '%s%s/%s'",
            database, table, BASE_PATH, database, table);
    executeSql("insert into table %s.%s VALUES (1, 'value1'), (2, 'value2')", database, table);
    Dataset<Row> rowDataset = executeSql(String.format("select * from %s.%s", database, table));
    List<Row> rows = rowDataset.collectAsList();
    assertThat(rows.size()).isEqualTo(2);
    assertThat(rows.get(0).get(0)).isEqualTo(1);
  }

  public static Dataset<Row> executeSql(String query, String... params) {
    return spark.sql(String.format(query, params));
  }
}
