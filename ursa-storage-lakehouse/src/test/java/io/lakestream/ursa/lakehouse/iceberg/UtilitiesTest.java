/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.iceberg;

import static io.lakestream.ursa.lakehouse.iceberg.Utilities.buildPartitionSpec;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.common.DynClasses;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Slf4j
@Tag("lakehouse")
public class UtilitiesTest {

  private static final String HADOOP_CONF_TEMPLATE =
      "<configuration><property><name>%s</name><value>%s</value></property></configuration>";

  private static final List<String> HADOOP_CONF_FILES =
      ImmutableList.of("core-site.xml", "hdfs-site.xml", "hive-site.xml");

  @TempDir private Path tempDir;

  private static final Schema TEST_SCHEMA = new Schema(
      Types.NestedField.required(1, "timestamp_col", Types.TimestampType.withoutZone()),
      Types.NestedField.required(2, "int_col", Types.IntegerType.get()),
      Types.NestedField.required(3, "string_col", Types.StringType.get())
  );
  private static final Schema NESTED_SCHEMA = new Schema(
      Types.NestedField.required(1, "id", Types.StringType.get()),
      Types.NestedField.required(2, "error", Types.StructType.of(
          Types.NestedField.required(3, "errorId", Types.StringType.get()),
          Types.NestedField.required(4, "receivedAt", Types.TimestampType.withoutZone()),
          Types.NestedField.required(5, "message", Types.StringType.get()),
          Types.NestedField.required(6, "app", Types.StructType.of(
              Types.NestedField.required(7, "version", Types.StringType.get()),
              Types.NestedField.required(8, "releaseDate", Types.DateType.get())
          ))
      )),
      Types.NestedField.required(9, "project", Types.StructType.of(
          Types.NestedField.required(10, "name", Types.StringType.get()),
          Types.NestedField.required(11, "createdAt", Types.TimestampType.withoutZone())
      ))
  );

  public static class TestCatalog extends InMemoryCatalog implements Configurable<Configuration> {
    private Configuration conf;

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
    }
  }

  @Test
  public void testLoadCatalogNoHadoopDir() {
    Map<String, String> props =
        ImmutableMap.of(
            "topics",
            "mytopic",
            "iceberg.tables",
            "mytable",
            "iceberg.hadoop.conf-prop",
            "conf-value",
            "iceberg.catalog.catalog-impl",
            TestCatalog.class.getName());
    Properties properties = new Properties();
    properties.putAll(props);

    IcebergSinkConfig config = new IcebergSinkConfig(properties);
    Catalog result = loadCatalog(config);

    assertThat(result).isInstanceOf(TestCatalog.class);

    Configuration conf = ((TestCatalog) result).conf;
    assertThat(conf).isNotNull();

    // check that the sink config property was added
    assertThat(conf.get("conf-prop")).isEqualTo("conf-value");

    // check that core-site.xml was loaded
    assertThat(conf.get("foo")).isEqualTo("bar");
  }

  @ParameterizedTest
  @ValueSource(strings = {"core-site.xml", "hdfs-site.xml", "hive-site.xml"})
  public void testLoadCatalogWithHadoopDir(String confFile) throws IOException {
    Path path = tempDir.resolve(confFile);
    String xml = String.format(HADOOP_CONF_TEMPLATE, "file-prop", "file-value");
    Files.write(path, xml.getBytes(StandardCharsets.UTF_8));

    Map<String, String> props =
        ImmutableMap.of(
            "topics",
            "mytopic",
            "iceberg.tables",
            "mytable",
            "iceberg.hadoop-conf-dir",
            tempDir.toString(),
            "iceberg.hadoop.conf-prop",
            "conf-value",
            "iceberg.catalog.catalog-impl",
            TestCatalog.class.getName());
    Properties properties = new Properties();
    properties.putAll(props);
    IcebergSinkConfig config = new IcebergSinkConfig(properties);
    Catalog result = loadCatalog(config);

    assertThat(result).isInstanceOf(TestCatalog.class);

    Configuration conf = ((TestCatalog) result).conf;
    assertThat(conf).isNotNull();

    // check that the sink config property was added
    assertThat(conf.get("conf-prop")).isEqualTo("conf-value");

    // check that the config file was loaded
    assertThat(conf.get("file-prop")).isEqualTo("file-value");

    // check that core-site.xml was loaded
    assertThat(conf.get("foo")).isEqualTo("bar");
  }

  public static Catalog loadCatalog(IcebergSinkConfig config) {
    return CatalogUtil.buildIcebergCatalog("ursa", config.getCatalogProps(), loadHadoopConfig(config));
  }


  // Test implementation of Record for verification
  static class TestRecord implements Record {
    private final Map<String, Object> fields = new HashMap<>();

    public TestRecord withField(String name, Object value) {
      fields.put(name, value);
      return this;
    }

    @Override
    public Object getField(String name) {
      return fields.get(name);
    }

    // Implement other Record methods with default behavior
    @Override public Types.StructType struct() {
      return null;
    }

    @Override public void setField(String name, Object value) {

    }

    @Override public Object get(int pos) {
      return null;
    }

    @Override public Record copy() {
      return null;
    }

    @Override public Record copy(Map<String, Object> overwriteValues) {
      return null;
    }

    @Override public int size() {
      return 0;
    }

    @Override public <T> T get(int pos, Class<T> javaClass) {
      return null;
    }

    @Override public <T> void set(int pos, T value) {

    }
  }

  @Test
  void shouldExtractTopLevelField() {
    TestRecord record = new TestRecord()
        .withField("name", "Alice")
        .withField("age", 30);

    Object result = Utilities.extractFromRecordValue(record, "name");
    assertThat(result).isEqualTo("Alice");
  }

  @Test
  void shouldHandleNestedFieldStructure() {
    TestRecord address = new TestRecord()
        .withField("city", "New York")
        .withField("zip", 10001);

    TestRecord record = new TestRecord()
        .withField("name", "Bob")
        .withField("address", address);

    Object result = Utilities.extractFromRecordValue(record, "address.city");
    assertThat(result).isEqualTo("New York");
  }

  @Test
  void shouldReturnNullForMissingField() {
    TestRecord record = new TestRecord()
        .withField("name", "Charlie");

    Object result = Utilities.extractFromRecordValue(record, "email");
    assertThat(result).isNull();
  }

  @Test
  void shouldHandleNullIntermediateField() {
    TestRecord record = new TestRecord()
        .withField("address", null);

    Object result = Utilities.extractFromRecordValue(record, "address.city");
    assertThat(result).isNull();
  }

  @Test
  void shouldHandleDeeplyNestedFields() {
    TestRecord country = new TestRecord()
        .withField("name", "USA")
        .withField("code", "US");

    TestRecord address = new TestRecord()
        .withField("city", "Los Angeles")
        .withField("country", country);

    TestRecord record = new TestRecord()
        .withField("name", "Dave")
        .withField("address", address);

    Object result = Utilities.extractFromRecordValue(record, "address.country.code");
    assertThat(result).isEqualTo("US");
  }

  @Test
  void shouldThrowForInvalidFieldPath() {
    TestRecord record = new TestRecord()
        .withField("name", "Eve");

    assertNull(Utilities.extractFromRecordValue(record, ""));
  }

  @Test
  void shouldHandleNonRecordIntermediateField() {
    TestRecord record = new TestRecord()
        .withField("metadata", "not_a_record");

    Object result = Utilities.extractFromRecordValue(record, "metadata.value");
    assertThat(result).isNull();
  }

  @Test
  void shouldHandleMixedTypeFields() {
    TestRecord details = new TestRecord()
        .withField("active", true)
        .withField("score", 95.5);

    TestRecord record = new TestRecord()
        .withField("id", 12345)
        .withField("details", details);

    assertThat(Utilities.extractFromRecordValue(record, "id"))
        .isEqualTo(12345);
    assertThat(Utilities.extractFromRecordValue(record, "details.active"))
        .isEqualTo(true);
    assertThat(Utilities.extractFromRecordValue(record, "details.score"))
        .isEqualTo(95.5);
  }

  // use reflection here to avoid requiring Hadoop as a dependency
  private static Object loadHadoopConfig(IcebergSinkConfig config) {
    Class<?> configClass = dynamicallyLoad("org.apache.hadoop.hdfs.HdfsConfiguration");
    if (configClass == null) {
      configClass = dynamicallyLoad("org.apache.hadoop.conf.Configuration");
    }

    if (configClass == null) {
      log.info("Hadoop not found on classpath, not creating Hadoop config");
      return null;
    }

    try {
      Object result = configClass.getDeclaredConstructor().newInstance();
      DynMethods.BoundMethod addResourceMethod =
          DynMethods.builder("addResource").impl(configClass, URL.class).build(result);
      DynMethods.BoundMethod setMethod =
          DynMethods.builder("set").impl(configClass, String.class, String.class).build(result);

      //  load any config files in the specified config directory
      String hadoopConfDir = config.hadoopConfDir();
      if (hadoopConfDir != null) {
        HADOOP_CONF_FILES.forEach(
            confFile -> {
              Path path = Paths.get(hadoopConfDir, confFile);
              if (Files.exists(path)) {
                try {
                  addResourceMethod.invoke(path.toUri().toURL());
                } catch (IOException e) {
                  log.warn("Error adding Hadoop resource {}, resource was not added", path, e);
                }
              }
            });
      }

      // set any Hadoop properties specified in the sink config
      config.getHadoopProps().forEach(setMethod::invoke);

      log.info("Hadoop config initialized: {}", configClass.getName());
      return result;
    } catch (InstantiationException
             | IllegalAccessException
             | NoSuchMethodException
             | InvocationTargetException e) {
      log.warn(
          "Hadoop found on classpath but could not create config, proceeding without config", e);
    }
    return null;
  }

  private static Class<?> dynamicallyLoad(String className) {
    Class<?> configClass;
    try {
      configClass = DynClasses.builder().impl(className).orNull().build();
    } catch (NoClassDefFoundError e) {
      configClass = null;
    }
    return configClass;
  }

  @Test
  public void testEmptyPartitionConfig() {
    // Test null partitionConfig
    IcebergPartitionSpec spec = buildPartitionSpec(TEST_SCHEMA, null);
    assertTrue(spec.getPartitionSpec().isUnpartitioned());
    assertTrue(spec.getExpressions().isEmpty());

    // Test empty partitionConfig
    spec = buildPartitionSpec(TEST_SCHEMA, Collections.emptyList());
    assertTrue(spec.getPartitionSpec().isUnpartitioned());
    assertTrue(spec.getExpressions().isEmpty());
  }

  @Test
  public void testInvalidSourceColumn() {
    IcebergPartitionConfig invalidConfig = new IcebergPartitionConfig("invalid_col", "year", "year_partition");
    IcebergPartitionSpec spec = buildPartitionSpec(TEST_SCHEMA, List.of(invalidConfig));
    assertTrue(spec.getPartitionSpec().isUnpartitioned());
    assertTrue(spec.getExpressions().isEmpty());
  }

  @Test
  public void testValidTransformsWithoutTargetName() {
    List<IcebergPartitionConfig> configs = List.of(
        new IcebergPartitionConfig("timestamp_col", "year", null),
        new IcebergPartitionConfig("int_col", "bucket[16]", null),
        new IcebergPartitionConfig("string_col", "truncate[5]", null)
    );

    IcebergPartitionSpec spec = buildPartitionSpec(TEST_SCHEMA, configs);
    assertEquals(3, spec.getPartitionSpec().fields().size());
    assertEquals(3, spec.getExpressions().size());

    // Verify year transform
    PartitionField yearField = spec.getPartitionSpec().fields().get(0);
    assertEquals("timestamp_col", getSourceName(yearField));
    assertEquals("year", yearField.transform().toString());
    assertEquals("timestamp_col_year", yearField.name());
    IcebergExpression expression = spec.getExpressions().get(0);
    assertNull(expression.targetName());
    assertEquals(Expressions.year("timestamp_col").toString(), expression.term().toString());

    // Verify bucket transform
    PartitionField bucketField = spec.getPartitionSpec().fields().get(1);
    assertEquals("int_col", getSourceName(bucketField));
    assertEquals("int_col_bucket", bucketField.name());
    expression = spec.getExpressions().get(1);
    assertNull(expression.targetName());
    assertEquals(Expressions.bucket("int_col", 16).toString(), expression.term().toString());

    // Verify truncate transform
    PartitionField truncateField = spec.getPartitionSpec().fields().get(2);
    assertEquals("string_col", getSourceName(truncateField));
    assertEquals("string_col_trunc", truncateField.name());
    expression = spec.getExpressions().get(2);
    assertNull(expression.targetName());
    assertEquals(Expressions.truncate("string_col", 5).toString(), expression.term().toString());
  }

  @Test
  public void testValidTransformsWithTargetName() {
    List<IcebergPartitionConfig> configs = List.of(
        new IcebergPartitionConfig("timestamp_col", "year", "ts_year"),
        new IcebergPartitionConfig("int_col", "bucket[16]", "int_bucket"),
        new IcebergPartitionConfig("string_col", "truncate[5]", "str_trunc")
    );

    IcebergPartitionSpec spec = buildPartitionSpec(TEST_SCHEMA, configs);
    assertEquals(3, spec.getPartitionSpec().fields().size());
    assertEquals(3, spec.getExpressions().size());

    PartitionField yearField = spec.getPartitionSpec().fields().get(0);
    assertEquals("timestamp_col", getSourceName(yearField));
    assertEquals("year", yearField.transform().toString());
    assertEquals("ts_year", yearField.name());
    IcebergExpression icebergExpression = spec.getExpressions().get(0);
    assertEquals("ts_year", icebergExpression.targetName());
    assertEquals(Expressions.year("timestamp_col").toString(), icebergExpression.term().toString());

    PartitionField bucketField = spec.getPartitionSpec().fields().get(1);
    assertEquals("int_col", getSourceName(bucketField));
    assertEquals("int_bucket", bucketField.name());
    icebergExpression = spec.getExpressions().get(1);
    assertEquals("int_bucket", icebergExpression.targetName());
    assertEquals(Expressions.bucket("int_col", 16).toString(), icebergExpression.term().toString());
    PartitionField truncateField = spec.getPartitionSpec().fields().get(2);
    assertEquals("string_col", getSourceName(truncateField));
    assertEquals("str_trunc", truncateField.name());
    icebergExpression = spec.getExpressions().get(2);
    assertEquals("str_trunc", icebergExpression.targetName());
    assertEquals(Expressions.truncate("string_col", 5).toString(), icebergExpression.term().toString());
  }

  @Test
  public void testValidTransformsWithTargetNameOnSameFeild() {
    List<IcebergPartitionConfig> configs = List.of(
        new IcebergPartitionConfig("timestamp_col", "year", "ts_year"),
        new IcebergPartitionConfig("timestamp_col", "month", "ts_month"),
        new IcebergPartitionConfig("timestamp_col", "day", "ts_day"),
        new IcebergPartitionConfig("timestamp_col", "hour", "ts_hour")
    );

    IcebergPartitionSpec spec = buildPartitionSpec(TEST_SCHEMA, configs);
    assertEquals(1, spec.getPartitionSpec().fields().size());
    assertEquals(1, spec.getExpressions().size());

    PartitionField yearField = spec.getPartitionSpec().fields().get(0);
    assertEquals("timestamp_col", getSourceName(yearField));
    assertEquals("year", yearField.transform().toString());
    assertEquals("ts_year", yearField.name());
    IcebergExpression icebergExpression = spec.getExpressions().get(0);
    assertEquals("ts_year", icebergExpression.targetName());
    assertEquals(Expressions.year("timestamp_col").toString(), icebergExpression.term().toString());
  }


  @Test
  public void testUnsupportedTransform() {
    IcebergPartitionConfig invalidConfig = new IcebergPartitionConfig("int_col", "invalid_transform", null);
    IcebergPartitionSpec spec = buildPartitionSpec(TEST_SCHEMA, List.of(invalidConfig));
    assertTrue(spec.getPartitionSpec().isUnpartitioned());
    assertTrue(spec.getExpressions().isEmpty());
  }

  @Test
  public void testMixedValidAndInvalidConfigs() {
    List<IcebergPartitionConfig> configs = List.of(
        new IcebergPartitionConfig("timestamp_col", "month", "ts_month"),
        new IcebergPartitionConfig("invalid_col", "day", "ts_day"),
        new IcebergPartitionConfig("int_col", "invalid_transform", null)
    );

    IcebergPartitionSpec spec = buildPartitionSpec(TEST_SCHEMA, configs);
    assertEquals(1, spec.getPartitionSpec().fields().size());
    assertEquals(1, spec.getExpressions().size());
    PartitionField monthField = spec.getPartitionSpec().fields().get(0);
    assertEquals("timestamp_col", getSourceName(monthField));
    assertEquals("month", monthField.transform().toString());
    assertEquals("ts_month", monthField.name());
    IcebergExpression icebergExpression = spec.getExpressions().get(0);
    assertEquals("ts_month", icebergExpression.targetName());
    assertEquals(Expressions.month("timestamp_col").toString(), icebergExpression.term().toString());
  }

  @Test
  public void testTransformCaseInsensitivity() {
    // Uppercase transform should be normalized to lowercase
    IcebergPartitionConfig config = new IcebergPartitionConfig("timestamp_col", "YEAR", "ts_year");
    IcebergPartitionSpec spec = buildPartitionSpec(TEST_SCHEMA, List.of(config));

    PartitionField yearField = spec.getPartitionSpec().fields().get(0);
    assertEquals("timestamp_col", getSourceName(yearField));
    assertEquals("year", yearField.transform().toString());
    assertEquals("ts_year", yearField.name());
    IcebergExpression icebergExpression = spec.getExpressions().get(0);
    assertEquals("ts_year", icebergExpression.targetName());
    assertEquals(Expressions.year("timestamp_col").toString(), icebergExpression.term().toString());
  }

  private String getSourceName(PartitionField field) {
    return TEST_SCHEMA.findField(field.sourceId()).name();
  }

  @Test
  public void testInvalidBucketAndTruncateTransforms() {
    // Test individual invalid bucket configurations
    List<IcebergPartitionConfig> invalidBucketConfigs = List.of(
        new IcebergPartitionConfig("int_col", "bucket", "invalid_bucket_missing_param"),  // Missing brackets
        new IcebergPartitionConfig("int_col", "bucket[abc]", "invalid_bucket_nan"),       // Non-numeric parameter
        new IcebergPartitionConfig("int_col", "bucket[0]", "invalid_bucket_zero"),        // Zero buckets
        new IcebergPartitionConfig("int_col", "bucket[-5]", "invalid_bucket_negative"),   // Negative buckets
        new IcebergPartitionConfig("int_col", "bucket[16", "invalid_bucket_open"),       // Missing closing bracket
        new IcebergPartitionConfig("int_col", "bucket16]", "invalid_bucket_close"),      // Missing opening bracket
        new IcebergPartitionConfig("int_col", "bucket[16]extra", "invalid_bucket_extra") // Extra characters
    );

    for (IcebergPartitionConfig config : invalidBucketConfigs) {
      IcebergPartitionSpec spec = buildPartitionSpec(TEST_SCHEMA, List.of(config));
      assertTrue(spec.getPartitionSpec().isUnpartitioned(),
          "Should skip invalid bucket transform: " + config.getTransform());
      assertEquals(0, spec.getExpressions().size());
      assertEquals(0, spec.getPartitionSpec().fields().size());

    }

    // Test individual invalid truncate configurations
    List<IcebergPartitionConfig> invalidTruncateConfigs = List.of(
        new IcebergPartitionConfig("string_col", "truncate", "invalid_trunc_missing_param"),
        new IcebergPartitionConfig("string_col", "truncate[abc]", "invalid_trunc_nan"),
        new IcebergPartitionConfig("string_col", "truncate[0]", "invalid_trunc_zero"),
        new IcebergPartitionConfig("string_col", "truncate[-5]", "invalid_trunc_negative"),
        new IcebergPartitionConfig("string_col", "truncate[5", "invalid_trunc_open"),
        new IcebergPartitionConfig("string_col", "truncate5]", "invalid_trunc_close"),
        new IcebergPartitionConfig("string_col", "truncate[5]extra", "invalid_trunc_extra")
    );

    for (IcebergPartitionConfig config : invalidTruncateConfigs) {
      IcebergPartitionSpec spec = buildPartitionSpec(TEST_SCHEMA, List.of(config));
      assertTrue(spec.getPartitionSpec().isUnpartitioned(),
          "Should skip invalid truncate transform: " + config.getTransform());
      assertEquals(0, spec.getExpressions().size());
      assertEquals(0, spec.getPartitionSpec().fields().size());
    }
  }

  @Test
  public void testMixedValidAndInvalidTransforms() {
    List<IcebergPartitionConfig> configs = List.of(
        // Valid transforms
        new IcebergPartitionConfig("int_col", "bucket[16]", "valid_bucket"),
        new IcebergPartitionConfig("string_col", "truncate[5]", "valid_truncate"),

        // Invalid transforms
        new IcebergPartitionConfig("int_col", "bucket[0]", "invalid_bucket"),
        new IcebergPartitionConfig("string_col", "truncate[abc]", "invalid_truncate"),

        // Another valid transform
        new IcebergPartitionConfig("timestamp_col", "year", "valid_year")
    );

    IcebergPartitionSpec spec = buildPartitionSpec(TEST_SCHEMA, configs);

    assertEquals(3, spec.getPartitionSpec().fields().size(), "Should create 3 valid partition fields");
    assertEquals(3, spec.getExpressions().size());

    assertPartitionFieldExists(spec.getPartitionSpec(), "valid_bucket");
    assertPartitionFieldExists(spec.getPartitionSpec(), "valid_truncate");
    assertPartitionFieldExists(spec.getPartitionSpec(), "valid_year");
  }

  private void assertPartitionFieldExists(PartitionSpec spec, String name) {
    assertTrue(spec.fields().stream().anyMatch(f -> f.name().equals(name)),
        "Should have partition field: " + name);
  }

  @Test
  public void testNestedFieldPartitioning() {
    List<IcebergPartitionConfig> configs = List.of(
        new IcebergPartitionConfig("error.receivedAt", "day", "error_received_day"),
        new IcebergPartitionConfig("error.app.releaseDate", "year", "app_release_year"),
        new IcebergPartitionConfig("project.createdAt", "month", null)
    );

    IcebergPartitionSpec spec = buildPartitionSpec(NESTED_SCHEMA, configs);

    assertEquals(3, spec.getPartitionSpec().fields().size(), "Should create 3 partition fields for nested fields");
    assertEquals(3, spec.getExpressions().size());

    // Verify partition fields
    assertPartitionFieldExists(spec.getPartitionSpec(), "error_received_day");
    assertPartitionFieldExists(spec.getPartitionSpec(), "app_release_year");
    // The third field should have auto-generated name since targetName is null
    assertTrue(spec.getPartitionSpec().fields().stream()
        .anyMatch(f -> f.sourceId() == 11 && f.transform().toString().equals("month")),
        "Should have month transform on project.createdAt");

    // Verify expressions
    assertEquals("error_received_day", spec.getExpressions().get(0).targetName());
    assertEquals(Expressions.day("error.receivedAt").toString(), spec.getExpressions().get(0).term().toString());

    assertEquals("app_release_year", spec.getExpressions().get(1).targetName());
    assertEquals(Expressions.year("error.app.releaseDate").toString(), spec.getExpressions().get(1).term().toString());
  }

  @Test
  public void testInvalidNestedFieldPartitioning() {
    List<IcebergPartitionConfig> configs = List.of(
        new IcebergPartitionConfig("error.nonExistentField", "day", "invalid_partition"),
        new IcebergPartitionConfig("nonExistentStruct.field", "year", "also_invalid")
    );
    IcebergPartitionSpec spec = buildPartitionSpec(NESTED_SCHEMA, configs);
    assertTrue(spec.getPartitionSpec().isUnpartitioned(), "Should be unpartitioned when all nested fields are invalid");
    assertTrue(spec.getExpressions().isEmpty());
  }

  @Test
  public void testMixedFlatAndNestedFieldPartitioning() {
    List<IcebergPartitionConfig> configs = List.of(
        new IcebergPartitionConfig("id", "truncate[4]", "id_prefix"),
        new IcebergPartitionConfig("error.receivedAt", "hour", "error_hour"),
        new IcebergPartitionConfig("project.name", "identity", "project_name_partition")
    );
    IcebergPartitionSpec spec = buildPartitionSpec(NESTED_SCHEMA, configs);
    assertEquals(3, spec.getPartitionSpec().fields().size(), "Should create partitions for both flat and nested fields");
    assertEquals(3, spec.getExpressions().size());
    assertPartitionFieldExists(spec.getPartitionSpec(), "id_prefix");
    assertPartitionFieldExists(spec.getPartitionSpec(), "error_hour");
    assertPartitionFieldExists(spec.getPartitionSpec(), "project.name");
  }
}
