package com.mogujie.mst.hbase.filters;

import com.mogujie.mst.hbase.HbaseTestBase;
import com.mogujie.mst.util.HbaseTestUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by fenqi on 16/6/18.
 */
public class RowFilterTest extends HbaseTestBase {
    private static final Logger log = LoggerFactory.getLogger(RowFilterTest.class);

    @Test
    public void testBinaryComparator() {
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes(keyPrefix + "4")));
        scanAndCheck(rowFilter, 1);
    }

    @Test
    public void testBinaryPrefixComparator() {
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryPrefixComparator(Bytes.toBytes(keyPrefix + "1")));
        scanAndCheck(rowFilter, 6);
    }

    @Test
    public void testSubstringComparator() {
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new SubstringComparator(keyPrefix + "15"));
        scanAndCheck(rowFilter, 0);
    }

    @Test
    public void testMultiRowRangeFilter() {
        List<MultiRowRangeFilter.RowRange> rowRangeList = new ArrayList<>();

        String row = "key0";
        byte[] startRowBytes = Bytes.toBytes(row);
        byte[] endRowBytes = HbaseTestUtil.getOneBitBiggerBytes(startRowBytes);

        log.info("startRowBytes: {}, endRowBytes: {}", new String(startRowBytes), new String(endRowBytes));

        MultiRowRangeFilter.RowRange rowRange =
                new MultiRowRangeFilter.RowRange(startRowBytes, true, endRowBytes, false);
        rowRangeList.add(rowRange);

        MultiRowRangeFilter multiRowRangeFilter = null;
        try {
            multiRowRangeFilter = new MultiRowRangeFilter(rowRangeList);
        } catch (IOException e) {
            Assert.fail(e.toString());
        }

        scanAndCheck(multiRowRangeFilter, 1);
    }

    @Test
    public void testFilterCombiner() {
        List<MultiRowRangeFilter.RowRange> rowRangeList = new ArrayList<>();
        List<String> rows = new ArrayList<>();
        rows.add("key0");
        rows.add("key4");
        rows.add("key7");

        for (String row : rows) {

            byte[] startRowBytes = Bytes.toBytes(row);
            byte[] endRowBytes = HbaseTestUtil.getOneBitBiggerBytes(startRowBytes);
            log.info("startRowBytes: {}, endRowBytes: {}", new String(startRowBytes), new String(endRowBytes));

            MultiRowRangeFilter.RowRange rowRange =
                    new MultiRowRangeFilter.RowRange(startRowBytes, true, endRowBytes, false);
            rowRangeList.add(rowRange);
        }

        MultiRowRangeFilter multiRowRangeFilter = null;
        try {
            multiRowRangeFilter = new MultiRowRangeFilter(rowRangeList);
        } catch (IOException e) {
            Assert.fail(e.toString());
        }

        String expr = "^.*[02468]$";
        int flag = Pattern.CASE_INSENSITIVE | Pattern.DOTALL;
        RegexStringComparator.EngineType engineType = RegexStringComparator.EngineType.JAVA;

        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                Bytes.toBytes(columnFamily),
                Bytes.toBytes(qualifier),
                CompareFilter.CompareOp.EQUAL,
                new RegexStringComparator(expr, flag, engineType)
        );

        FilterList filterList = new FilterList();
        filterList.addFilter(multiRowRangeFilter);
        filterList.addFilter(singleColumnValueFilter);

        scanAndCheck(filterList, 2);
    }

}
