package io.openlineage.spark.agent;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.HashMap;

public class MetastoreTestUtils {
  private static final String LOCAL_IP = "127.0.0.1";
  private static final String VERSION = System.getProperty("spark.version");
  private static final String BASE_PATH = "gs://gidasttn-dev-bucket/warehouse/" + VERSION + "/";
  //    private static final String BASE_PATH = "gs://openlineage-ci-testing/warehouse/" + VERSION +
  // "/";
  private static final String GCLOUD_KEY = "GCLOUD_SERVICE_KEY";
  private static final String PROJECT_ID = "GOOGLE_PROJECT_ID";
  private static final Map<String, String> GOOGLE_SA_PROPERTIES = parseGoogleSAProperties();

  public static final int POSTGRES_PORT = 5432;

  public static SparkConf getCommonSparkConf(
      String appName, String metastoreName, int mappedPort, Boolean isIceberg) {
    String projectId = System.getenv(PROJECT_ID);
    SparkConf conf =
        new SparkConf()
            .setAppName(appName + VERSION)
            .setMaster("local[*]")
            .set("spark.driver.host", LOCAL_IP)
            .set("org.jpox.autoCreateSchema", "true")
            .set(
                "javax.jdo.option.ConnectionURL",
                String.format("jdbc:postgresql://localhost:%d/%s", mappedPort, metastoreName))
            .set("javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver")
            .set("javax.jdo.option.ConnectionUserName", "hiveuser")
            .set("javax.jdo.option.ConnectionPassword", "password")
            .set("spark.sql.warehouse.dir", BASE_PATH)
            .set("spark.hadoop.google.cloud.project.id", projectId)
            .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
            .set(
                "fs.gs.auth.service.account.private.key.id",
                GOOGLE_SA_PROPERTIES.get("private_key_id"))
            .set("fs.gs.auth.service.account.private.key", GOOGLE_SA_PROPERTIES.get("private_key"))
            .set("fs.gs.auth.service.account.email", GOOGLE_SA_PROPERTIES.get("client_email"))
            .set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            .set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");

    if (isIceberg) {
      conf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
          .set("spark.sql.catalog.spark_catalog.type", "hive")
          .set(
              "spark.sql.extensions",
              "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
    }
    return conf;
  }

  public static Map<String, String> parseGoogleSAProperties() {
    String json = System.getenv(GCLOUD_KEY);
    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, String> gcsProperties;
    try {
      gcsProperties = objectMapper.readValue(json, Map.class);
    } catch (IOException e) {
      throw new RuntimeException("Couldn't parse properties from GCLOUD_KEY", e);
    }
    return gcsProperties;
  }

  public static Configuration getFileSystemConf(SparkSession spark) {
    Map<String, String> saProperties = MetastoreTestUtils.parseGoogleSAProperties();
    Configuration conf = spark.sparkContext().hadoopConfiguration();
    conf.set("fs.gs.auth.service.account.private.key.id", saProperties.get("private_key_id"));
    conf.set("fs.gs.auth.service.account.private.key", saProperties.get("private_key"));
    conf.set("fs.gs.auth.service.account.email", saProperties.get("client_email"));
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    return conf;
  }

  public static void removeBaseDir(String pathString, FileSystem fs) {
    try {
      Path path = new Path(pathString);
      if (fs.exists(path)) {
        fs.delete(path, true);
      }
    } catch (IOException e) {
      throw new RuntimeException("Couldn't remove directory " + pathString, e);
    }
  }

  public static FileSystem getFileSystem(SparkSession spark) {
    try {
      return new Path(BASE_PATH).getFileSystem(MetastoreTestUtils.getFileSystemConf(spark));
    } catch (IOException e) {
      throw new RuntimeException("Couldn't get file system", e);
    }
  }

  public static void createSampleFiles(
      SparkSession spark, String basePath, String database, String table, Boolean isIceberg) {
    StructType structTypeSchema =
        new StructType(
            new StructField[] {
              new StructField("id", IntegerType$.MODULE$, false, new Metadata(new HashMap<>())),
              new StructField("value", IntegerType$.MODULE$, false, new Metadata(new HashMap<>()))
            });
    List<Row> rows =
        Arrays.asList(new GenericRow(new Object[] {1, 2}), new GenericRow(new Object[] {3, 4}));
    Dataset<Row> dataFrame = spark.createDataFrame(rows, structTypeSchema);
    String pathTemplate;
    if (isIceberg) {
      pathTemplate = "%s%s/%s/data";
    } else {
      pathTemplate = "%s%s/%s";
    }

    dataFrame.write().parquet(String.format(pathTemplate, basePath, database, table));
  }
}
