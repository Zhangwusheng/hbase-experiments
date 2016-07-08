package com.mogujie.mst.hbase;

import com.mogujie.mst.util.StringGenerator;
import com.mogujie.mst.util.StringListGenerator;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by fenqi on 16/6/30.
 */
public class HbaseTestBase {
    private static final Logger log = LoggerFactory.getLogger(HbaseTestBase.class);

    protected static HbaseOperator operator = null;
    protected static Table table = null;

    protected static String tableName = "t1",
            columnFamily = "cf",
            qualifier = "q";
    protected static String keyPrefix = "key", valuePrefix = "value";
    protected static int count = 15;
    protected static String zookeeperURI = "localhost";
    protected static boolean truncateTable = true, dropTable = false;
    protected static StringGenerator keyGenerator, valueGenerator;
    protected static StringListGenerator qualifierGenerator;

    @BeforeClass
    public static void beforeClass() {

        if (null == keyGenerator) {
            log.info("init keyGenerator by default...");
            keyGenerator = (seeds) -> seeds[2] + seeds[1] + seeds[0];
        }

        if (null == valueGenerator) {
            log.info("init valueGenerator by default...");
            valueGenerator = (seeds) -> seeds[2] + seeds[1] + seeds[0];
        }

        if (null == qualifierGenerator) {
            log.info("init qualifierGenerator by default...");
            qualifierGenerator = (seeds) -> seeds;
        }

        operator = new HbaseOperator();
        operator.init(zookeeperURI);

        // clean data
        try {
            createTable(tableName, columnFamily, truncateTable);
        } catch (IOException e) {
            Assert.fail(e.toString());
        }

        try {
            table = operator.getConnection().getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            Assert.fail(e.toString());
        }

        // warm-up
        for (int i = 0; i < count; i++) {
            String rowKey = keyGenerator.get(String.valueOf(i), "", keyPrefix),
                    value = valueGenerator.get(String.valueOf(i), "", valuePrefix);

            Put put = new Put(Bytes.toBytes(rowKey));
            String[] qualifiers = qualifierGenerator.get(qualifier);
            for (int j = 0; j < qualifiers.length; j++) {
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifiers[j]), Bytes.toBytes(value));
            }

            try {
                table.put(put);
            } catch (IOException e) {
                Assert.fail(e.toString());
            }
        }

        if (null != table) {
            try {
                table.close();
            } catch (IOException e) {
                Assert.fail(e.toString());
            }
        }

    }

    public static void createTable(String strTableName, String strColumnFamily, boolean ifTruncateTable) throws IOException {
        Admin admin = operator.getConnection().getAdmin();
        TableName table = TableName.valueOf(strTableName);

        boolean tableExist = admin.tableExists(table);
        if (tableExist) {
            if (ifTruncateTable) {
                log.info("table {} exist, truncate...", strTableName);
                admin.disableTable(table);
                if (admin.isTableDisabled(table)) {
                    admin.truncateTable(table, true);
                }
            }
        } else {
            log.info("table {} not exist, create...", strTableName);

            HColumnDescriptor family = new HColumnDescriptor(strColumnFamily);

            HTableDescriptor hTableDescriptor = new HTableDescriptor(table);
            hTableDescriptor.addFamily(family);

            admin.createTable(hTableDescriptor);
        }
    }

    @AfterClass
    public static void afterClass() {
        log.info("{}");
        try {
            if (null != operator) {
                dropTable(tableName, dropTable);
                operator.close();
            }
        } catch (IOException e) {
            Assert.fail(e.toString());
        }
    }

    public static void dropTable(String strTableName, boolean ifDropTable) throws IOException {
        if (ifDropTable) {
            log.info("drop table {}...", strTableName);
            Admin admin = operator.getConnection().getAdmin();
            TableName table = TableName.valueOf(strTableName);
            admin.disableTable(table);
            if (admin.isTableDisabled(table)) {
                admin.deleteTable(table);
            }
        }
    }

    @Before
    public void before() {
        log.info("{}");
        try {
            table = operator.getConnection().getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            Assert.fail(e.toString());
        }
    }

    @After
    public void after() {
        log.info("{}");
        if (null != table) {
            try {
                table.close();
            } catch (IOException e) {
                Assert.fail(e.toString());
            }
        }
    }

    public void scanAndCheck(Filter filter, int expectRowNum,
                             String startKey, String endKey,
                             int expectCellNum, String columnFamily) {
        Scan scan = new Scan();
        scan.setFilter(filter);

        if (null != startKey) {
            scan.setStartRow(Bytes.toBytes(startKey));
        }

        if (null != endKey) {
            scan.setStopRow(Bytes.toBytes(endKey));
        }

        if (null != columnFamily) {
            scan.addFamily(Bytes.toBytes(columnFamily));
        }

        ResultScanner scanner;
        try {
            scanner = table.getScanner(scan);
            int rowNum = 0;
            for (Result result = scanner.next(); result != null; rowNum++, result = scanner.next()) {
                log.info(new String(result.getRow()));
                if (0 < expectCellNum) {
                    log.info(result.listCells().toString());
                    Assert.assertEquals(expectCellNum, result.listCells().size());
                }
            }
            scanner.close();
            Assert.assertEquals(expectRowNum, rowNum);
        } catch (IOException e) {
            Assert.fail(e.toString());
        }
    }

    public void scanAndCheck(Filter filter, int expectValue) {
        scanAndCheck(filter, expectValue,
                null, null,
                -1, null);

    }

}
