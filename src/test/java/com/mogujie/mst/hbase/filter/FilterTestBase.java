package com.mogujie.mst.hbase.filter;

import com.mogujie.mst.HbaseOperator;
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
public class FilterTestBase {
    private static final Logger log = LoggerFactory.getLogger(FilterTestBase.class);

    private static HbaseOperator operator = null;
    protected static String tableName = "t1",
            columnFamily = "cf",
            qualifier = "q";

    protected static String keyPrefix = "key", valuePrefix = "value";
    private static Table table = null;
    private static int count = 15;
    private static String zookeeperURI = "localhost";
    private static boolean truncateTable = true, dropTable = false;

    protected static StringGenerator keyGenerator, valueGenerator;

    static class DefaultStringGenerator implements StringGenerator {
        @Override
        public String get(String... seeds) {
            return seeds[2] + seeds[1] + seeds[0];
        }
    }

    @BeforeClass
    public static void beforeClass() {

        if (null == keyGenerator) {
            log.info("init keyGenerator by default...");
            keyGenerator = new DefaultStringGenerator();
        }

        if (null == valueGenerator) {
            log.info("init valueGenerator by default...");
            valueGenerator = new DefaultStringGenerator();
        }

        operator = new HbaseOperator();
        operator.init(zookeeperURI);

        // clean data
        try {
            Admin admin = operator.getConnection().getAdmin();
            TableName table = TableName.valueOf(tableName);

            boolean tableExist = admin.tableExists(table);
            if (tableExist) {
                if (truncateTable) {
                    log.info("table {} exist, truncate...", tableName);
                    admin.disableTable(table);
                    if (admin.isTableDisabled(table)) {
                        admin.truncateTable(table, true);
                    }
                }
            } else {
                log.info("table {} not exist, create...", tableName);

                HColumnDescriptor family = new HColumnDescriptor(columnFamily);

                HTableDescriptor hTableDescriptor = new HTableDescriptor(table);
                hTableDescriptor.addFamily(family);

                admin.createTable(hTableDescriptor);
            }
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
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
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

    @AfterClass
    public static void afterClass() {
        log.info("{}");
        try {
            if (null != operator) {
                if (dropTable) {
                    log.info("drop table {}...", tableName);
                    Admin admin = operator.getConnection().getAdmin();
                    TableName table = TableName.valueOf(tableName);
                    admin.disableTable(table);
                    if (admin.isTableDisabled(table)) {
                        admin.deleteTable(table);
                    }
                }
                operator.close();
            }
        } catch (IOException e) {
            Assert.fail(e.toString());
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

    public void scanAndCheck(Filter filter, int expectValue) {
        Scan scan = new Scan();
        scan.setFilter(filter);

        ResultScanner scanner;
        try {
            scanner = table.getScanner(scan);
            int resultNum = 0;
            for (Result result = scanner.next(); result != null; resultNum++, result = scanner.next()) {
                log.info(new String(result.getRow()));
            }
            scanner.close();
            Assert.assertEquals(expectValue, resultNum);
        } catch (IOException e) {
            Assert.fail(e.toString());
        }
    }

}
