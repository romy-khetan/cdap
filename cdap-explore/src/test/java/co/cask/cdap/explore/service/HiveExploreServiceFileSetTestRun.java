/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.explore.service;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.test.SlowTests;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionAware;
import org.apache.twill.filesystem.Location;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.text.DateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * This tests that time partitioned file sets and their partitions are correctly registered
 * in the Hive meta store when created, and also that they are removed from Hive when deleted.
 * This does not test querying through Hive (it will be covered by an integration test).
 */
@Category(SlowTests.class)
public class HiveExploreServiceFileSetTestRun extends BaseHiveExploreServiceTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final Schema SCHEMA = Schema.recordOf("kv",
                                                       Schema.Field.of("key", Schema.of(Schema.Type.STRING)),
                                                       Schema.Field.of("value", Schema.of(Schema.Type.STRING)));
  private static final Schema K_SCHEMA = Schema.recordOf("k",
                                                         Schema.Field.of("key", Schema.of(Schema.Type.STRING)));
  private static final DateFormat DATE_FORMAT = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT,
                                                                               Locale.US);

  @BeforeClass
  public static void start() throws Exception {
    initialize(CConfiguration.create(), tmpFolder, false);
  }

  @After
  public void deleteAll() throws Exception {
    datasetFramework.deleteAllInstances(NAMESPACE_ID);
  }

  @Test
  public void testOrcFileset() throws Exception {
    final Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(NAMESPACE_ID, "orcfiles");
    final String tableName = getDatasetHiveName(datasetInstanceId);

    // create a time partitioned file set
    datasetFramework.addInstance("fileSet", datasetInstanceId, FileSetProperties.builder()
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.ql.io.orc.OrcSerde")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat")
      .setExploreSchema("id int, name string")
      .build());

    // verify that the hive table was created for this file set
    runCommand(NAMESPACE_ID, "show tables", true,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList(tableName))));

    // insert data into the table

    ExploreExecutionResult result = exploreClient.submit(
      NAMESPACE_ID, String.format("insert into table %s values (1, 'samuel'), (2, 'dwayne')", tableName)).get();
    result.close();

    // verify that we can query the key-values in the file with Hive
    runCommand(NAMESPACE_ID, "SELECT * FROM " + tableName, true,
               Lists.newArrayList(
                 new ColumnDesc(tableName + ".id", "INT", 1, null),
                 new ColumnDesc(tableName + ".name", "STRING", 2, null)),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList(1, "samuel")),
                 new QueryResult(Lists.<Object>newArrayList(2, "dwayne"))));

    // drop the dataset
    datasetFramework.deleteInstance(datasetInstanceId);

    // verify the Hive table is gone
    runCommand(NAMESPACE_ID, "show tables", false,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Collections.<QueryResult>emptyList());
  }

  @Test
  public void testCreateAddDrop() throws Exception {
    final Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(NAMESPACE_ID, "files");
    final String tableName = getDatasetHiveName(datasetInstanceId);

    // create a time partitioned file set
    datasetFramework.addInstance("fileSet", datasetInstanceId, FileSetProperties.builder()
      // properties for file set
      .setBasePath("myPath")
        // properties for partitioned hive table
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", SCHEMA.toString())
      .build());

    // verify that the hive table was created for this file set
    runCommand(NAMESPACE_ID, "show tables", true,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList(tableName))));

    // Accessing dataset instance to perform data operations
    FileSet fileSet = datasetFramework.getDataset(datasetInstanceId, DatasetDefinition.NO_ARGUMENTS, null);
    Assert.assertNotNull(fileSet);

    // add a file
    FileWriterHelper.generateAvroFile(fileSet.getLocation("file1").getOutputStream(), "a", 0, 3);

    // verify that we can query the key-values in the file with Hive
    runCommand(NAMESPACE_ID, "SELECT * FROM " + tableName, true,
               Lists.newArrayList(
                 new ColumnDesc(tableName + ".key", "STRING", 1, null),
                 new ColumnDesc(tableName + ".value", "STRING", 2, null)),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("a0", "#0")),
                 new QueryResult(Lists.<Object>newArrayList("a1", "#1")),
                 new QueryResult(Lists.<Object>newArrayList("a2", "#2"))));

    // add another file
    FileWriterHelper.generateAvroFile(fileSet.getLocation("file2").getOutputStream(), "b", 3, 5);

    // verify that we can query the key-values in the file with Hive
    runCommand(NAMESPACE_ID, "SELECT count(*) AS count FROM " + tableName, true,
               Lists.newArrayList(new ColumnDesc("count", "BIGINT", 1, null)),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList(5L))));

    // drop the dataset
    datasetFramework.deleteInstance(datasetInstanceId);

    // verify the Hive table is gone
    runCommand(NAMESPACE_ID, "show tables", false,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Collections.<QueryResult>emptyList());
  }


  @Test
  public void testPartitionedFileSet() throws Exception {
    final Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(NAMESPACE_ID, "parted");
    final String tableName = getDatasetHiveName(datasetInstanceId);

    // create a time partitioned file set
    datasetFramework.addInstance("partitionedFileSet", datasetInstanceId, PartitionedFileSetProperties.builder()
      .setPartitioning(Partitioning.builder()
                         .addStringField("str")
                         .addIntField("num")
                         .build())
        // properties for file set
      .setBasePath("parted")
        // properties for partitioned hive table
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", SCHEMA.toString())
      .build());

    // verify that the hive table was created for this file set
    runCommand(NAMESPACE_ID, "show tables", true,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList(tableName))));

    // Accessing dataset instance to perform data operations
    PartitionedFileSet partitioned = datasetFramework.getDataset(datasetInstanceId, DatasetDefinition.NO_ARGUMENTS,
                                                                 null);
    Assert.assertNotNull(partitioned);
    FileSet fileSet = partitioned.getEmbeddedFileSet();

    // add some partitions. Beware that Hive expects a partition to be a directory, so we create dirs with one file
    Location locationX1 = fileSet.getLocation("fileX1/nn");
    Location locationY1 = fileSet.getLocation("fileY1/nn");
    Location locationX2 = fileSet.getLocation("fileX2/nn");
    Location locationY2 = fileSet.getLocation("fileY2/nn");

    FileWriterHelper.generateAvroFile(locationX1.getOutputStream(), "x", 1, 2);
    FileWriterHelper.generateAvroFile(locationY1.getOutputStream(), "y", 1, 2);
    FileWriterHelper.generateAvroFile(locationX2.getOutputStream(), "x", 2, 3);
    FileWriterHelper.generateAvroFile(locationY2.getOutputStream(), "y", 2, 3);

    PartitionKey keyX1 = PartitionKey.builder().addStringField("str", "x").addIntField("num", 1).build();
    PartitionKey keyY1 = PartitionKey.builder().addStringField("str", "y").addIntField("num", 1).build();
    PartitionKey keyX2 = PartitionKey.builder().addStringField("str", "x").addIntField("num", 2).build();
    PartitionKey keyY2 = PartitionKey.builder().addStringField("str", "y").addIntField("num", 2).build();

    addPartition(partitioned, keyX1, "fileX1");
    addPartition(partitioned, keyY1, "fileY1");
    addPartition(partitioned, keyX2, "fileX2");
    addPartition(partitioned, keyY2, "fileY2");

    // verify that the partitions were added to Hive
    runCommand(NAMESPACE_ID, "show partitions " + tableName, true,
               Lists.newArrayList(new ColumnDesc("partition", "STRING", 1, "from deserializer")),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("str=x/num=1")),
                 new QueryResult(Lists.<Object>newArrayList("str=x/num=2")),
                 new QueryResult(Lists.<Object>newArrayList("str=y/num=1")),
                 new QueryResult(Lists.<Object>newArrayList("str=y/num=2"))));

    // verify that we can query the key-values in the file with Hive
    runCommand(NAMESPACE_ID, "SELECT count(*) AS count FROM " + tableName, true,
               Lists.newArrayList(new ColumnDesc("count", "BIGINT", 1, null)),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList(4L))));

    // verify that we can query the key-values in the file with Hive
    runCommand(NAMESPACE_ID, "SELECT * FROM " + tableName + " ORDER BY key, value", true,
               Lists.newArrayList(
                 new ColumnDesc(tableName + ".key", "STRING", 1, null),
                 new ColumnDesc(tableName + ".value", "STRING", 2, null),
                 new ColumnDesc(tableName + ".str", "STRING", 3, null),
                 new ColumnDesc(tableName + ".num", "INT", 4, null)),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("x1", "#1", "x", 1)),
                 new QueryResult(Lists.<Object>newArrayList("x2", "#2", "x", 2)),
                 new QueryResult(Lists.<Object>newArrayList("y1", "#1", "y", 1)),
                 new QueryResult(Lists.<Object>newArrayList("y2", "#2", "y", 2))));

    // verify that we can query the key-values in the file with Hive
    runCommand(NAMESPACE_ID, "SELECT * FROM " + tableName + " WHERE num = 2 ORDER BY key, value", true,
               Lists.newArrayList(
                 new ColumnDesc(tableName + ".key", "STRING", 1, null),
                 new ColumnDesc(tableName + ".value", "STRING", 2, null),
                 new ColumnDesc(tableName + ".str", "STRING", 3, null),
                 new ColumnDesc(tableName + ".num", "INT", 4, null)),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("x2", "#2", "x", 2)),
                 new QueryResult(Lists.<Object>newArrayList("y2", "#2", "y", 2))));

    // drop a partition and query again
    dropPartition(partitioned, keyX2);
    validatePartitions(partitioned, ImmutableSet.of(keyX1, keyY1, keyY2));

    // verify that one value is gone now, namely x2
    runCommand(NAMESPACE_ID, "SELECT key, value FROM " + tableName + " ORDER BY key, value", true,
               Lists.newArrayList(
                 new ColumnDesc("key", "STRING", 1, null),
                 new ColumnDesc("value", "STRING", 2, null)),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("x1", "#1")),
                 new QueryResult(Lists.<Object>newArrayList("y1", "#1")),
                 new QueryResult(Lists.<Object>newArrayList("y2", "#2"))));

    // drop a partition directly from hive
    runCommand(NAMESPACE_ID, "ALTER TABLE " + tableName + " DROP PARTITION (str='y', num=2)", false, null, null);
    // verify that one more value is gone now, namely y2
    runCommand(NAMESPACE_ID, "SELECT key, value FROM " + tableName + " ORDER BY key, value", true,
               Lists.newArrayList(
                 new ColumnDesc("key", "STRING", 1, null),
                 new ColumnDesc("value", "STRING", 2, null)),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("x1", "#1")),
                 new QueryResult(Lists.<Object>newArrayList("y1", "#1"))));
    // make sure the partition can still be dropped from the PFS dataset
    validatePartitions(partitioned, ImmutableSet.of(keyX1, keyY1, keyY2));
    dropPartition(partitioned, keyY2);
    validatePartitions(partitioned, ImmutableSet.of(keyX1, keyY1));

    // drop the dataset
    datasetFramework.deleteInstance(datasetInstanceId);

    // verify the Hive table is gone
    runCommand(NAMESPACE_ID, "show tables", false,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Collections.<QueryResult>emptyList());
  }

  @Test
  public void testPartitionedTextFile() throws Exception {
    testPartitionedTextFile("csv", "csv", null, ",");
    testPartitionedTextFile("null", "text", null, "\1"); // default used by Hive if not delimiter given
    testPartitionedTextFile("comma", "text", ",", ",");
    testPartitionedTextFile("blank", "text", " ", " ");
  }

  // this tests mainly the support for different text formats. Other features (partitioning etc.) are tested above.
  private void testPartitionedTextFile(String name, String format, String delim, String fileDelim) throws Exception {
    final Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(NAMESPACE_ID, name);
    final String tableName = getDatasetHiveName(datasetInstanceId);
    // create a time partitioned file set
    PartitionedFileSetProperties.Builder builder = (PartitionedFileSetProperties.Builder)
      PartitionedFileSetProperties.builder()
        .setPartitioning(Partitioning.builder().addIntField("number").build())
        // properties for file set
        .setBasePath(name)
        // properties for partitioned hive table
        .setEnableExploreOnCreate(true)
        .setExploreSchema("key STRING, value INT")
        .setExploreFormat(format);
    if (delim != null) {
      builder.setExploreFormatProperty("delimiter", delim);
    }
    datasetFramework.addInstance("partitionedFileSet", datasetInstanceId, builder.build());

    // verify that the hive table was created for this file set
    runCommand(NAMESPACE_ID, "show tables", true,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList(tableName))));

    // Accessing dataset instance to perform data operations
    PartitionedFileSet partitioned = datasetFramework.getDataset(datasetInstanceId, DatasetDefinition.NO_ARGUMENTS,
                                                                 null);
    Assert.assertNotNull(partitioned);
    FileSet fileSet = partitioned.getEmbeddedFileSet();

    // add a partitions. Beware that Hive expects a partition to be a directory, so we create a dir with one file
    Location location1 = fileSet.getLocation("file1/nn");
    FileWriterHelper.generateTextFile(location1.getOutputStream(), fileDelim, "x", 1, 2);
    PartitionKey key1 = PartitionKey.builder().addIntField("number", 1).build();
    addPartition(partitioned, key1, "file1");

    // verify that the partitions were added to Hive
    runCommand(NAMESPACE_ID, "show partitions " + tableName, true,
               Lists.newArrayList(new ColumnDesc("partition", "STRING", 1, "from deserializer")),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList("number=1"))));

    // verify that we can query the key-values in the file with Hive
    runCommand(NAMESPACE_ID, "SELECT * FROM " + tableName + " ORDER BY key", true,
               Lists.newArrayList(
                 new ColumnDesc(tableName + ".key", "STRING", 1, null),
                 new ColumnDesc(tableName + ".value", "INT", 2, null),
                 new ColumnDesc(tableName + ".number", "INT", 3, null)),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("x1", 1, 1))));

    // drop a partition and query again
    dropPartition(partitioned, key1);

    // drop the dataset
    datasetFramework.deleteInstance(datasetInstanceId);

    // verify the Hive table is gone
    runCommand(NAMESPACE_ID, "show tables", false,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Collections.<QueryResult>emptyList());
  }

  @Test
  public void testPartitionedAvroSchemaUpdate() throws Exception {
    final Id.DatasetInstance datasetId = Id.DatasetInstance.from(NAMESPACE_ID, "avroupd");
    final String tableName = getDatasetHiveName(datasetId);

    // create a time partitioned file set
    datasetFramework.addInstance(PartitionedFileSet.class.getName(), datasetId, PartitionedFileSetProperties.builder()
      .setPartitioning(Partitioning.builder().addIntField("number").build())
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", SCHEMA.toString())
      .build());

    // Accessing dataset instance to perform data operations
    PartitionedFileSet partitioned = datasetFramework.getDataset(datasetId, DatasetDefinition.NO_ARGUMENTS, null);
    Assert.assertNotNull(partitioned);
    FileSet fileSet = partitioned.getEmbeddedFileSet();

    // add a partition
    Location location4 = fileSet.getLocation("file4/nn");
    FileWriterHelper.generateAvroFile(location4.getOutputStream(), "x", 4, 5);
    addPartition(partitioned, PartitionKey.builder().addIntField("number", 4).build(), "file4");

    // new partition should have new format, validate with query
    List<ColumnDesc> expectedColumns = Lists.newArrayList(
      new ColumnDesc(tableName + ".key", "STRING", 1, null),
      new ColumnDesc(tableName + ".value", "STRING", 2, null),
      new ColumnDesc(tableName + ".number", "INT", 3, null));
    runCommand(NAMESPACE_ID, "SELECT * FROM " + tableName + " WHERE number=4", true,
               expectedColumns,
               Lists.newArrayList(
                 // avro file has key=x4, value=#4
                 new QueryResult(Lists.<Object>newArrayList("x4", "#4", 4))));

    // create a time partitioned file set
    datasetFramework.updateInstance(datasetId, PartitionedFileSetProperties.builder()
      .setPartitioning(Partitioning.builder().addIntField("number").build())
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", K_SCHEMA.toString())
      .build());
    expectedColumns = Lists.newArrayList(
      new ColumnDesc(tableName + ".key", "STRING", 1, null),
      new ColumnDesc(tableName + ".number", "INT", 2, null));
    runCommand(NAMESPACE_ID, "SELECT * FROM " + tableName + " WHERE number=4", true,
               expectedColumns,
               Lists.newArrayList(
                 // avro file has key=x4, value=#4
                 new QueryResult(Lists.<Object>newArrayList("x4", 4))));
  }

  @Test
  public void testPartitionedTextSchemaUpdate() throws Exception {
    final Id.DatasetInstance datasetId = Id.DatasetInstance.from(NAMESPACE_ID, "txtschemaupd");
    final String tableName = getDatasetHiveName(datasetId);

    // create a time partitioned file set
    datasetFramework.addInstance(PartitionedFileSet.class.getName(), datasetId, PartitionedFileSetProperties.builder()
      .setPartitioning(Partitioning.builder().addIntField("number").build())
      .setEnableExploreOnCreate(true)
      .setExploreSchema("key STRING, value STRING")
      .setExploreFormat("csv")
      .build());

    // verify that the hive table was created for this file set
    runCommand(NAMESPACE_ID, "show tables", true,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList(tableName))));

    // Accessing dataset instance to perform data operations
    PartitionedFileSet partitioned = datasetFramework.getDataset(datasetId, DatasetDefinition.NO_ARGUMENTS, null);
    Assert.assertNotNull(partitioned);
    FileSet fileSet = partitioned.getEmbeddedFileSet();

    // add a partitions. Beware that Hive expects a partition to be a directory, so we create a dir with one file
    Location location1 = fileSet.getLocation("file1/nn");
    FileWriterHelper.generateMultiDelimitersFile(location1.getOutputStream(), ImmutableList.of(",", "\1", ":"), 1, 2);
    addPartition(partitioned, PartitionKey.builder().addIntField("number", 1).build(), "file1");

    // verify that the partitions were added to Hive
    runCommand(NAMESPACE_ID, "show partitions " + tableName, true,
               Lists.newArrayList(new ColumnDesc("partition", "STRING", 1, "from deserializer")),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList("number=1"))));

    // verify that we can query the key-values in the file with Hive.
    List<ColumnDesc> expectedColumns = Lists.newArrayList(
      new ColumnDesc(tableName + ".key", "STRING", 1, null),
      new ColumnDesc(tableName + ".value", "STRING", 2, null),
      new ColumnDesc(tableName + ".number", "INT", 3, null));
    runCommand(NAMESPACE_ID, "SELECT * FROM " + tableName + " WHERE number=1", true,
               expectedColumns,
               Lists.newArrayList(
                 // text line has the form 1,x\1x:1, format is csv -> key=1 value=x\1x:1
                 new QueryResult(Lists.<Object>newArrayList("1", "x\1x:1", 1))));

    // update the dataset properties with a different delimiter
    datasetFramework.updateInstance(datasetId, PartitionedFileSetProperties.builder()
      .setPartitioning(Partitioning.builder().addIntField("number").build())
      .setEnableExploreOnCreate(true)
      .setExploreSchema("str STRING")
      .setExploreFormat("csv")
      .build());
    // new partition should have new schema, validate with query
    expectedColumns = Lists.newArrayList(
      new ColumnDesc(tableName + ".str", "STRING", 1, null),
      new ColumnDesc(tableName + ".number", "INT", 2, null));
    runCommand(NAMESPACE_ID, "SELECT * FROM " + tableName + " WHERE number=1", true,
               expectedColumns,
               Lists.newArrayList(
                 // text line has the form 1,x\1x:1, format is csv -> key=1 value=x\1x:1
                 new QueryResult(Lists.<Object>newArrayList("1", 1))));
  }

  @Test
    public void testPartitionedTextFileUpdate() throws Exception {
    final Id.DatasetInstance datasetId = Id.DatasetInstance.from(NAMESPACE_ID, "txtupd");
    final String tableName = getDatasetHiveName(datasetId);

    // create a time partitioned file set
    datasetFramework.addInstance(PartitionedFileSet.class.getName(), datasetId, PartitionedFileSetProperties.builder()
      .setPartitioning(Partitioning.builder().addIntField("number").build())
      .setEnableExploreOnCreate(true)
      .setExploreSchema("key STRING, value STRING")
      .setExploreFormat("csv")
      .build());

    // verify that the hive table was created for this file set
    runCommand(NAMESPACE_ID, "show tables", true,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList(tableName))));

    // Accessing dataset instance to perform data operations
    PartitionedFileSet partitioned = datasetFramework.getDataset(datasetId, DatasetDefinition.NO_ARGUMENTS, null);
    Assert.assertNotNull(partitioned);
    FileSet fileSet = partitioned.getEmbeddedFileSet();

    // add a partitions. Beware that Hive expects a partition to be a directory, so we create a dir with one file
    Location location1 = fileSet.getLocation("file1/nn");
    FileWriterHelper.generateMultiDelimitersFile(location1.getOutputStream(), ImmutableList.of(",", "\1", ":"), 1, 2);
    addPartition(partitioned, PartitionKey.builder().addIntField("number", 1).build(), "file1");

    // verify that the partitions were added to Hive
    runCommand(NAMESPACE_ID, "show partitions " + tableName, true,
               Lists.newArrayList(new ColumnDesc("partition", "STRING", 1, "from deserializer")),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList("number=1"))));

    // verify that we can query the key-values in the file with Hive.
    List<ColumnDesc> expectedColumns = Lists.newArrayList(
      new ColumnDesc(tableName + ".key", "STRING", 1, null),
      new ColumnDesc(tableName + ".value", "STRING", 2, null),
      new ColumnDesc(tableName + ".number", "INT", 3, null));
    runCommand(NAMESPACE_ID, "SELECT * FROM " + tableName + " WHERE number=1", true,
               expectedColumns,
               Lists.newArrayList(
                 // text line has the form 1,x\1x:1, format is csv -> key=1 value=x\1x:1
                 new QueryResult(Lists.<Object>newArrayList("1", "x\1x:1", 1))));

    // update the dataset properties with a different delimiter
    datasetFramework.updateInstance(datasetId, PartitionedFileSetProperties.builder()
      .setPartitioning(Partitioning.builder().addIntField("number").build())
      .setEnableExploreOnCreate(true)
      .setExploreSchema("key STRING, value STRING")
      .setExploreFormat("text")
      .build());
    // add another partition
    Location location2 = fileSet.getLocation("file2/nn");
    FileWriterHelper.generateMultiDelimitersFile(location2.getOutputStream(), ImmutableList.of(",", "\1", ":"), 2, 3);
    addPartition(partitioned, PartitionKey.builder().addIntField("number", 2).build(), "file2");
    // new partition should have new format, validate with query
    runCommand(NAMESPACE_ID, "SELECT * FROM " + tableName + " WHERE number=2", true,
               expectedColumns,
               Lists.newArrayList(
                 // text line has the form 2,x\1x:2, format is text -> key=2,x value=x:2
                 new QueryResult(Lists.<Object>newArrayList("2,x", "x:2", 2))));

    // update the dataset properties with a different delimiter
    datasetFramework.updateInstance(datasetId, PartitionedFileSetProperties.builder()
      .setPartitioning(Partitioning.builder().addIntField("number").build())
      .setEnableExploreOnCreate(true)
      .setExploreSchema("key STRING, value STRING")
      .setExploreFormat("text")
      .setExploreFormatProperty("delimiter", ":")
      .build());
    // add another partition
    Location location3 = fileSet.getLocation("file3/nn");
    FileWriterHelper.generateMultiDelimitersFile(location3.getOutputStream(), ImmutableList.of(",", "\1", ":"), 3, 4);
    addPartition(partitioned, PartitionKey.builder().addIntField("number", 3).build(), "file3");
    // new partition should have new format, validate with query
    runCommand(NAMESPACE_ID, "SELECT * FROM " + tableName + " WHERE number=3", true,
               expectedColumns,
               Lists.newArrayList(
                 // text line has the form 2,x\1x:2, format is text -> key=3,x\1x value=3
                 new QueryResult(Lists.<Object>newArrayList("3,x\1x", "3", 3))));

    // update the dataset properties with a different format (avro)
    datasetFramework.updateInstance(datasetId, PartitionedFileSetProperties.builder()
      .setPartitioning(Partitioning.builder().addIntField("number").build())
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", SCHEMA.toString())
      .build());
    // add another partition
    Location location4 = fileSet.getLocation("file4/nn");
    FileWriterHelper.generateAvroFile(location4.getOutputStream(), "x", 4, 5);
    addPartition(partitioned, PartitionKey.builder().addIntField("number", 4).build(), "file4");
    // new partition should have new format, validate with query
    runCommand(NAMESPACE_ID, "SELECT * FROM " + tableName + " WHERE number=4", true,
               expectedColumns,
               Lists.newArrayList(
                 // avro file has key=x4, value=#4
                 new QueryResult(Lists.<Object>newArrayList("x4", "#4", 4))));
  }

  @Test
  public void testTimePartitionedFileSet() throws Exception {
    final Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(NAMESPACE_ID, "parts");
    final String tableName = getDatasetHiveName(datasetInstanceId);

    // create a time partitioned file set
    datasetFramework.addInstance("timePartitionedFileSet", datasetInstanceId, FileSetProperties.builder()
      // properties for file set
      .setBasePath("somePath")
        // properties for partitioned hive table
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", SCHEMA.toString())
      .build());

    // verify that the hive table was created for this file set
    runCommand(NAMESPACE_ID, "show tables", true,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList(tableName))));

    // Accessing dataset instance to perform data operations
    TimePartitionedFileSet tpfs = datasetFramework.getDataset(datasetInstanceId, DatasetDefinition.NO_ARGUMENTS, null);
    Assert.assertNotNull(tpfs);
    Assert.assertTrue(tpfs instanceof TransactionAware);

    // add some partitions. Beware that Hive expects a partition to be a directory, so we create dirs with one file
    long time1 = DATE_FORMAT.parse("12/10/14 1:00 am").getTime();
    long time2 = DATE_FORMAT.parse("12/10/14 2:00 am").getTime();
    long time3 = DATE_FORMAT.parse("12/10/14 3:00 am").getTime();

    Location location1 = tpfs.getEmbeddedFileSet().getLocation("file1/nn");
    Location location2 = tpfs.getEmbeddedFileSet().getLocation("file2/nn");
    Location location3 = tpfs.getEmbeddedFileSet().getLocation("file3/nn");

    FileWriterHelper.generateAvroFile(location1.getOutputStream(), "x", 1, 2);
    FileWriterHelper.generateAvroFile(location2.getOutputStream(), "y", 2, 3);
    FileWriterHelper.generateAvroFile(location3.getOutputStream(), "x", 3, 4);

    addTimePartition(tpfs, time1, "file1");
    addTimePartition(tpfs, time2, "file2");
    addTimePartition(tpfs, time3, "file3");

    // verify that the partitions were added to Hive
    runCommand(NAMESPACE_ID, "show partitions " + tableName, true,
               Lists.newArrayList(new ColumnDesc("partition", "STRING", 1, "from deserializer")),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("year=2014/month=12/day=10/hour=1/minute=0")),
                 new QueryResult(Lists.<Object>newArrayList("year=2014/month=12/day=10/hour=2/minute=0")),
                 new QueryResult(Lists.<Object>newArrayList("year=2014/month=12/day=10/hour=3/minute=0"))));

    // verify that we can query the key-values in the file with Hive
    runCommand(NAMESPACE_ID, "SELECT key, value FROM " + tableName + " ORDER BY key, value", true,
               Lists.newArrayList(
                 new ColumnDesc("key", "STRING", 1, null),
                 new ColumnDesc("value", "STRING", 2, null)),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("x1", "#1")),
                 new QueryResult(Lists.<Object>newArrayList("x3", "#3")),
                 new QueryResult(Lists.<Object>newArrayList("y2", "#2"))));

    // verify that we can query the key-values in the file with Hive
    runCommand(NAMESPACE_ID, "SELECT key, value FROM " + tableName + " WHERE hour = 2 ORDER BY key, value",
               true,
               Lists.newArrayList(
                 new ColumnDesc("key", "STRING", 1, null),
                 new ColumnDesc("value", "STRING", 2, null)),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("y2", "#2"))));

    // remove a partition
    dropTimePartition(tpfs, time2);

    // verify that we can query the key-values in the file with Hive
    runCommand(NAMESPACE_ID, "SELECT key, value FROM " + tableName + " ORDER BY key, value", true,
               Lists.newArrayList(
                 new ColumnDesc("key", "STRING", 1, null),
                 new ColumnDesc("value", "STRING", 2, null)),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("x1", "#1")),
                 new QueryResult(Lists.<Object>newArrayList("x3", "#3"))));

    // verify the partition was removed from Hive
    // verify that the partitions were added to Hive
    runCommand(NAMESPACE_ID, "show partitions " + tableName, true,
               Lists.newArrayList(new ColumnDesc("partition", "STRING", 1, "from deserializer")),
               Lists.newArrayList(
                 new QueryResult(Lists.<Object>newArrayList("year=2014/month=12/day=10/hour=1/minute=0")),
                 new QueryResult(Lists.<Object>newArrayList("year=2014/month=12/day=10/hour=3/minute=0"))));

    // drop the dataset
    datasetFramework.deleteInstance(datasetInstanceId);

    // verify the Hive table is gone
    runCommand(NAMESPACE_ID, "show tables", false,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Collections.<QueryResult>emptyList());

    datasetFramework.addInstance("timePartitionedFileSet", datasetInstanceId, FileSetProperties.builder()
      // properties for file set
      .setBasePath("somePath")
        // properties for partitioned hive table
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", SCHEMA.toString())
      .build());

    // verify that the hive table was created for this file set
    runCommand(NAMESPACE_ID, "show tables", true,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Lists.newArrayList(new QueryResult(Lists.<Object>newArrayList(tableName))));
  }

  private void addPartition(final PartitionedFileSet partitioned, final PartitionKey key, final String path)
    throws Exception {
    doTransaction(partitioned, new Runnable() {
      @Override
      public void run() {
        partitioned.addPartition(key, path);
      }
    });
  }

  private void dropPartition(final PartitionedFileSet partitioned, final PartitionKey key) throws Exception {
    doTransaction(partitioned, new Runnable() {
      @Override
      public void run() {
        partitioned.dropPartition(key);
      }
    });
  }

  private void validatePartitions(final PartitionedFileSet partitioned,
                                  final Collection<PartitionKey> expected)
    throws Exception {
    doTransaction(partitioned, new Runnable() {
      @Override
      public void run() {
        Set<PartitionDetail> actual = partitioned.getPartitions(null);
        Assert.assertEquals(ImmutableSet.copyOf(expected),
                            ImmutableSet.copyOf(Iterables.transform(
                              actual,
                              new Function<PartitionDetail, PartitionKey>() {
                                @Nullable
                                @Override
                                public PartitionKey apply(@Nullable PartitionDetail detail) {
                                  Assert.assertNotNull(detail);
                                  return detail.getPartitionKey();
                                }
                              })));
      }
    });
  }

  private void addTimePartition(final TimePartitionedFileSet tpfs, final long time, final String path)
    throws Exception {
    doTransaction(tpfs, new Runnable() {
      @Override
      public void run() {
        tpfs.addPartition(time, path);
      }
    });
  }

  private void dropTimePartition(final TimePartitionedFileSet tpfs, final long time) throws Exception {
    doTransaction(tpfs, new Runnable() {
      @Override
      public void run() {
        tpfs.dropPartition(time);
      }
    });
  }

  private void doTransaction(Dataset tpfs, Runnable runnable) throws Exception {
    TransactionAware txAware = (TransactionAware) tpfs;
    Transaction tx = transactionManager.startShort(100);
    txAware.startTx(tx);
    runnable.run();
    Assert.assertTrue(txAware.commitTx());
    Assert.assertTrue(transactionManager.canCommit(tx, txAware.getTxChanges()));
    Assert.assertTrue(transactionManager.commit(tx));
    txAware.postTxCommit();
  }

}
