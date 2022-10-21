package io.openlineage.spark.agent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import scala.collection.immutable.HashMap;

@Slf4j
@Tag("integration-test")
@Testcontainers
public class MetastoreHive3Test {
    private static final String LOCAL_IP = "127.0.0.1";
    private static final String VERSION = System.getProperty("spark.version");
    private static final String database = "hive3";
    private static final String table = "test";
    private static final String BASE_PATH = "gs://gidasttn-dev-bucket/warehouse/" + VERSION + "/";
//    private static final String BASE_PATH = "gs://openlineage-ci-testing/warehouse/" + VERSION + "/";
    private static final String GCLOUD_KEY = "GCLOUD_SERVICE_KEY";
    private static final String PROJECT_ID = "GOOGLE_PROJECT_ID";
    private static final int ORIGINAL_PORT = 5432;
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
        mappedPort = metastoreContainer.getMappedPort(ORIGINAL_PORT);
        spark = SparkSession.builder()
                .config(getCommonSparkConf("MetastoreHive3Test", "metastore31"))
                .enableHiveSupport()
                .getOrCreate();
        try {
            Path basePath = new Path(BASE_PATH);
            fs = basePath.getFileSystem(getConf());
            if(fs.exists(basePath)) {
                fs.delete(basePath,true);
            }
        } catch (IOException e) {
            throw new RuntimeException("Couldn't remove directory " + BASE_PATH, e);
        }

        StructType structTypeSchema =
                new StructType(
                        new StructField[] {
                                new StructField("id", IntegerType$.MODULE$, false, new Metadata(new HashMap<>())),
                                new StructField("value", IntegerType$.MODULE$, false, new Metadata(new HashMap<>()))
                        });
        List<Row> rows = Arrays.asList(new GenericRow(new Object[]{1, 2}), new GenericRow(new Object[]{3, 4}));
        Dataset<Row> dataFrame = spark.createDataFrame(rows, structTypeSchema);
        dataFrame.write().format("avro").save(String.format("%s/%s/%s",BASE_PATH, database, table));
        executeSql("DROP TABLE IF EXISTS %s.%s", database, table);
        executeSql("DROP DATABASE IF EXISTS %s", database);
    }

    private static Configuration getConf() {
        String json = System.getenv(GCLOUD_KEY);
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> m;
        try {
            m = objectMapper.readValue(json, Map.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        Configuration conf = spark.sparkContext().hadoopConfiguration();
        conf.set("fs.gs.auth.service.account.private.key.id", m.get("private_key_id"));
        conf.set("fs.gs.auth.service.account.private.key", m.get("private_key"));
        conf.set("fs.gs.auth.service.account.email", m.get("client_email"));
        conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
        conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
        return conf;
    }

    @AfterAll
    public static void tearDown() {
        metastoreContainer.stop();
        spark.close();
        Path basePath = new Path(BASE_PATH);
        try {
            if(fs.exists(basePath)) {
                fs.delete(basePath,true);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static SparkConf getCommonSparkConf(String appName, String metastore) {
        String json = System.getenv(GCLOUD_KEY);
        String projectId = System.getenv(PROJECT_ID);
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> m;
        try {
            m = objectMapper.readValue(json, Map.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        
        return new SparkConf()
                .setAppName(appName + VERSION)
                .setMaster("local[*]")
                .set("spark.driver.host", LOCAL_IP)
                .set("org.jpox.autoCreateSchema", "true")
                .set(
                        "javax.jdo.option.ConnectionURL",
                        String.format("jdbc:postgresql://localhost:%d/%s", mappedPort, metastore))
                .set("javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver")
                .set("javax.jdo.option.ConnectionUserName", "hiveuser")
                .set("javax.jdo.option.ConnectionPassword", "password")
                .set("spark.sql.warehouse.dir", BASE_PATH)
                .set("spark.hadoop.google.cloud.project.id", projectId)
                .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
                .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .set("spark.sql.catalog.spark_catalog.type", "hive")
                .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .set("fs.gs.auth.service.account.private.key.id", m.get("private_key_id"))
                .set("fs.gs.auth.service.account.private.key", m.get("private_key"))
                .set("fs.gs.auth.service.account.email", m.get("client_email"))
                .set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
                .set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
        
    }

      @Test
      void IcebergTablesTest() {
        HiveCatalog catalog = new HiveCatalog();
        catalog.setConf(spark.sparkContext().hadoopConfiguration());
        executeSql("create database if not exists %s", database);
        executeSql("drop table if exists %s.%s", database, table);
        executeSql("create external table %s.%s (id int, value string) USING iceberg location '%s/%s/%s'",
                database, table, BASE_PATH, database, table);
        executeSql(String.format("select * from %s.%s", database, table)).show();
//        spark.close();

        List<TableIdentifier> tables = catalog.listTables(Namespace.of(database));
        Table table1 = catalog.loadTable(tables.get(0));
      }

    public static Dataset<Row> executeSql(String query, String... params) {
        return spark.sql(String.format(query, params));
    }
}
