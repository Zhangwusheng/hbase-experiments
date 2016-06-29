package com.mogujie.mst.hbase.filter;

import com.mogujie.mst.HbaseOperator;
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

    protected static String keyPrefix = "row", valuePrefix = "v";
    private static Table table = null;
    private static int count = 15;
    private static String zookeeperURI = "localhost";

    private static StringGenerator keyGenerator, valueGenerator;

    static class DefaultStringGenerator implements StringGenerator {
        @Override
        public String get(String... seeds) {
            return seeds[2] + seeds[1] + seeds[0];
        }
    }

    static {
        keyGenerator = valueGenerator = new DefaultStringGenerator();
    }

    @BeforeClass
    public static void beforeClass() {
        operator = new HbaseOperator();
        operator.init(zookeeperURI);

        try {
            table = operator.getConnection().getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            Assert.fail(e.getMessage());
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
                Assert.fail(e.getMessage());
            }
        }

        if (null != table) {
            try {
                table.close();
            } catch (IOException e) {
                Assert.fail(e.getMessage());
            }
        }

    }

    @AfterClass
    public static void afterClass() {
        if (null != operator) {
            operator.close();
        }
    }

    @Before
    public void before() {
        try {
            table = operator.getConnection().getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @After
    public void after() {
        if (null != table) {
            try {
                table.close();
            } catch (IOException e) {
                Assert.fail(e.getMessage());
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
            Assert.fail(e.getMessage());
        }
    }

}
