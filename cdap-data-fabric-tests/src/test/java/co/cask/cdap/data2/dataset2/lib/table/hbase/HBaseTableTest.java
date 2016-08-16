/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table.hbase;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.dataset.table.Tables;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.namespace.SimpleNamespaceQueryAdmin;
import co.cask.cdap.data.hbase.HBaseTestBase;
import co.cask.cdap.data.hbase.HBaseTestFactory;
import co.cask.cdap.data2.dataset2.lib.table.BufferingTable;
import co.cask.cdap.data2.dataset2.lib.table.BufferingTableTest;
import co.cask.cdap.data2.increment.hbase.IncrementHandlerState;
import co.cask.cdap.data2.increment.hbase98.IncrementHandler;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.SlowTests;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.tephra.DefaultTransactionExecutor;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.inmemory.DetachedTxSystemClient;
import org.apache.twill.filesystem.FileContextLocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 */
@Category(SlowTests.class)
public class HBaseTableTest extends BufferingTableTest<BufferingTable> {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseTableTest.class);

  @ClassRule
  public static final HBaseTestBase TEST_HBASE = new HBaseTestFactory().get();

  private static HBaseTableUtil hBaseTableUtil;
  private static CConfiguration cConf;

  @BeforeClass
  public static void beforeClass() throws Exception {
    cConf = CConfiguration.create();
    hBaseTableUtil = new HBaseTableUtilFactory(cConf, new SimpleNamespaceQueryAdmin()).get();
    // TODO: CDAP-1634 - Explore a way to not have every HBase test class do this.
    hBaseTableUtil.createNamespaceIfNotExists(TEST_HBASE.getHBaseAdmin(),
                                              hBaseTableUtil.getHBaseNamespace(NAMESPACE1.toEntityId()));
    hBaseTableUtil.createNamespaceIfNotExists(TEST_HBASE.getHBaseAdmin(),
                                              hBaseTableUtil.getHBaseNamespace(NAMESPACE2.toEntityId()));
  }

  @AfterClass
  public static void afterClass() throws Exception {
    hBaseTableUtil.deleteAllInNamespace(TEST_HBASE.getHBaseAdmin(),
                                        hBaseTableUtil.getHBaseNamespace(NAMESPACE1.toEntityId()));
    hBaseTableUtil.deleteAllInNamespace(TEST_HBASE.getHBaseAdmin(),
                                        hBaseTableUtil.getHBaseNamespace(NAMESPACE2.toEntityId()));
    hBaseTableUtil.deleteNamespaceIfExists(TEST_HBASE.getHBaseAdmin(),
                                           hBaseTableUtil.getHBaseNamespace(NAMESPACE1.toEntityId()));
    hBaseTableUtil.deleteNamespaceIfExists(TEST_HBASE.getHBaseAdmin(),
                                           hBaseTableUtil.getHBaseNamespace(NAMESPACE2.toEntityId()));
  }

  @Override
  protected BufferingTable getTable(DatasetContext datasetContext, String name,
                                    ConflictDetection conflictLevel) throws Exception {
    // ttl=-1 means "keep data forever"
    DatasetSpecification spec = DatasetSpecification.builder(name, "foo")
      .property(Table.PROPERTY_READLESS_INCREMENT, "true")
      .property(Table.PROPERTY_CONFLICT_LEVEL, conflictLevel.name())
      .build();
    return new HBaseTable(datasetContext, spec, cConf, TEST_HBASE.getConfiguration(), hBaseTableUtil);
  }

  @Override
  protected HBaseTableAdmin getTableAdmin(DatasetContext datasetContext, String name,
                                          DatasetProperties props) throws IOException {
    DatasetSpecification spec = new HBaseTableDefinition("foo").configure(name, props);
    return getTableAdmin(datasetContext, spec);
  }

  protected HBaseTableAdmin getTableAdmin(DatasetContext datasetContext, DatasetSpecification spec)
    throws IOException {
    return new HBaseTableAdmin(datasetContext, spec, TEST_HBASE.getConfiguration(), hBaseTableUtil,
                               cConf, new FileContextLocationFactory(TEST_HBASE.getConfiguration()));
  }

  @Override
  protected boolean isReadlessIncrementSupported() {
    return true;
  }

  @Test
  public void testTTL() throws Exception {
    // for the purpose of this test it is fine not to configure ttl when creating table: we want to see if it
    // applies on reading
    int ttl = 1;
    String ttlTable = "ttl";
    String noTtlTable = "nottl";
    DatasetProperties props = DatasetProperties.builder().add(Table.PROPERTY_TTL, String.valueOf(ttl)).build();
    getTableAdmin(CONTEXT1, ttlTable, props).create();
    DatasetSpecification ttlTableSpec = DatasetSpecification.builder(ttlTable, HBaseTable.class.getName())
      .properties(props.getProperties())
      .build();
    HBaseTable table = new HBaseTable(CONTEXT1, ttlTableSpec, cConf, TEST_HBASE.getConfiguration(), hBaseTableUtil);

    DetachedTxSystemClient txSystemClient = new DetachedTxSystemClient();
    Transaction tx = txSystemClient.startShort();
    table.startTx(tx);
    table.put(b("row1"), b("col1"), b("val1"));
    table.commitTx();

    TimeUnit.MILLISECONDS.sleep(1010);

    tx = txSystemClient.startShort();
    table.startTx(tx);
    table.put(b("row2"), b("col2"), b("val2"));
    table.commitTx();

    // now, we should not see first as it should have expired, but see the last one
    tx = txSystemClient.startShort();
    table.startTx(tx);
    byte[] val = table.get(b("row1"), b("col1"));
    if (val != null) {
      LOG.info("Unexpected value " + Bytes.toStringBinary(val));
    }
    Assert.assertNull(val);
    Assert.assertArrayEquals(b("val2"), table.get(b("row2"), b("col2")));

    // test a table with no TTL
    DatasetProperties props2 = DatasetProperties.builder()
      .add(Table.PROPERTY_TTL, String.valueOf(Tables.NO_TTL)).build();
    getTableAdmin(CONTEXT1, noTtlTable, props2).create();
    DatasetSpecification noTtlTableSpec = DatasetSpecification.builder(noTtlTable, HBaseTable.class.getName())
      .properties(props2.getProperties())
      .build();
    HBaseTable table2 = new HBaseTable(CONTEXT1, noTtlTableSpec, cConf, TEST_HBASE.getConfiguration(), hBaseTableUtil);

    tx = txSystemClient.startShort();
    table2.startTx(tx);
    table2.put(b("row1"), b("col1"), b("val1"));
    table2.commitTx();

    TimeUnit.SECONDS.sleep(2);

    tx = txSystemClient.startShort();
    table2.startTx(tx);
    table2.put(b("row2"), b("col2"), b("val2"));
    table2.commitTx();

    // if ttl is -1 (unlimited), it should see both
    tx = txSystemClient.startShort();
    table2.startTx(tx);
    Assert.assertArrayEquals(b("val1"), table2.get(b("row1"), b("col1")));
    Assert.assertArrayEquals(b("val2"), table2.get(b("row2"), b("col2")));
  }

  @Test
  public void testPreSplit() throws Exception {
    byte[][] splits = new byte[][] {Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c")};
    DatasetProperties props = DatasetProperties.builder().add("hbase.splits", new Gson().toJson(splits)).build();
    String presplittedTable = "presplitted";
    getTableAdmin(CONTEXT1, presplittedTable, props).create();

    try (HBaseAdmin hBaseAdmin = TEST_HBASE.getHBaseAdmin()) {
      TableId hTableId = hBaseTableUtil.createHTableId(NAMESPACE1.toEntityId(), presplittedTable);
      List<HRegionInfo> regions = hBaseTableUtil.getTableRegions(hBaseAdmin, hTableId);
      // note: first region starts at very first row key, so we have one extra to the splits count
      Assert.assertEquals(4, regions.size());
      Assert.assertArrayEquals(Bytes.toBytes("a"), regions.get(1).getStartKey());
      Assert.assertArrayEquals(Bytes.toBytes("b"), regions.get(2).getStartKey());
      Assert.assertArrayEquals(Bytes.toBytes("c"), regions.get(3).getStartKey());
    }
  }

  @Test
  public void testEnableIncrements() throws Exception {
    // setup a table with increments disabled and with it enabled
    String disableTableName = "incr-disable";
    String enabledTableName = "incr-enable";
    TableId disabledTableId = hBaseTableUtil.createHTableId(NAMESPACE1.toEntityId(), disableTableName);
    TableId enabledTableId = hBaseTableUtil.createHTableId(NAMESPACE1.toEntityId(), enabledTableName);
    HBaseTableAdmin disabledAdmin = getTableAdmin(CONTEXT1, disableTableName, DatasetProperties.EMPTY);
    disabledAdmin.create();
    HBaseAdmin admin = TEST_HBASE.getHBaseAdmin();

    DatasetProperties props =
      DatasetProperties.builder().add(Table.PROPERTY_READLESS_INCREMENT, "true").build();
    HBaseTableAdmin enabledAdmin = getTableAdmin(CONTEXT1, enabledTableName, props);
    enabledAdmin.create();

    try {

      try {
        HTableDescriptor htd = hBaseTableUtil.getHTableDescriptor(admin, disabledTableId);
        List<String> cps = htd.getCoprocessors();
        assertFalse(cps.contains(IncrementHandler.class.getName()));

        htd = hBaseTableUtil.getHTableDescriptor(admin, enabledTableId);
        cps = htd.getCoprocessors();
        assertTrue(cps.contains(IncrementHandler.class.getName()));
      } finally {
        admin.close();
      }

      BufferingTable table = getTable(CONTEXT1, enabledTableName, ConflictDetection.COLUMN);
      byte[] row = Bytes.toBytes("row1");
      byte[] col = Bytes.toBytes("col1");
      DetachedTxSystemClient txSystemClient = new DetachedTxSystemClient();
      Transaction tx = txSystemClient.startShort();
      table.startTx(tx);
      table.increment(row, col, 10);
      table.commitTx();
      // verify that value was written as a delta value
      final byte[] expectedValue = Bytes.add(IncrementHandlerState.DELTA_MAGIC_PREFIX, Bytes.toBytes(10L));
      final AtomicBoolean foundValue = new AtomicBoolean();
      byte [] enabledTableNameBytes = hBaseTableUtil.getHTableDescriptor(admin, enabledTableId).getName();
      TEST_HBASE.forEachRegion(enabledTableNameBytes, new Function<HRegion, Object>() {
        @Override
        public Object apply(HRegion hRegion) {
          Scan scan = hBaseTableUtil.buildScan().build();
          try {
            RegionScanner scanner = hRegion.getScanner(scan);
            List<Cell> results = Lists.newArrayList();
            boolean hasMore;
            do {
              hasMore = scanner.next(results);
              for (Cell cell : results) {
                if (CellUtil.matchingValue(cell, expectedValue)) {
                  foundValue.set(true);
                }
              }
            } while (hasMore);
          } catch (IOException ioe) {
            fail("IOException scanning region: " + ioe.getMessage());
          }
          return null;
        }
      });
      assertTrue("Should have seen the expected encoded delta value in the " + enabledTableName + " table region",
                 foundValue.get());
    } finally {
      disabledAdmin.drop();
      enabledAdmin.drop();
    }
  }

  @Test
  public void testColumnFamily() throws Exception {
    DatasetProperties props = DatasetProperties.builder().add(Table.PROPERTY_COLUMN_FAMILY, "t").build();
    HBaseTableDefinition tableDefinition = new HBaseTableDefinition("foo");
    String tableName = "testcf";
    DatasetSpecification spec = tableDefinition.configure(tableName, props);
    DatasetAdmin admin = getTableAdmin(CONTEXT1, spec);
    admin.create();
    final HBaseTable table = new HBaseTable(CONTEXT1, spec, cConf, TEST_HBASE.getConfiguration(), hBaseTableUtil);

    TransactionSystemClient txClient = new DetachedTxSystemClient();
    TransactionExecutor executor = new DefaultTransactionExecutor(txClient, table);
    executor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        table.put(new Put("row", "column", "testValue"));
      }
    });

    final HBaseTable table2 = new HBaseTable(CONTEXT1, spec, cConf, TEST_HBASE.getConfiguration(), hBaseTableUtil);
    executor = new DefaultTransactionExecutor(txClient, table2);
    executor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals("testValue", table2.get(new Get("row", "column")).getString("column"));
      }
    });

    // Verify the column family name
    TableId hTableId = hBaseTableUtil.createHTableId(new NamespaceId(CONTEXT1.getNamespaceId()), tableName);
    HTableDescriptor htd = hBaseTableUtil.getHTableDescriptor(TEST_HBASE.getHBaseAdmin(), hTableId);
    HColumnDescriptor hcd = htd.getFamily(Bytes.toBytes("t"));
    Assert.assertNotNull(hcd);
    Assert.assertEquals("t", hcd.getNameAsString());
  }

  private static byte[] b(String s) {
    return Bytes.toBytes(s);
  }

}
