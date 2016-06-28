package com.mogujie.mst;

import com.mogujie.mst.filters.CustomFilterForRishi;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

/**
 * Created by fenqi on 16/6/18.
 */
public class RowFilterTest {
    private static final Logger log = LoggerFactory.getLogger(RowFilterTest.class);
    private static HbaseOperator operator = null;
    private static String tableName = "t1",
            columnFamily = "cf",
            qualifier = "q",
            keyPrefix = "row";
    private static Table table = null;

    @BeforeClass
    public static void beforeClass() {
        operator = new HbaseOperator();
        operator.init("localhost");

        try {
            table = operator.getConnection().getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }

        // warm-up
        int count = 15;
        for (int i = 0; i < count; i++) {
            String rowKey = keyPrefix + i,
                    value = "v" + i;
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

    @Test
    public void testBinaryComparator() {
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(keyPrefix + "4")));
        scanAndCheck(rowFilter, 1);
    }

    @Test
    public void testBinaryPrefixComparator() {
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes(keyPrefix + "1")));
        scanAndCheck(rowFilter, 6);
    }

    @Test
    public void testSubstringComparator() {
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(keyPrefix + "15"));
        scanAndCheck(rowFilter, 0);
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
