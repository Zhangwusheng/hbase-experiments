package com.mogujie.mst.hbase.filters;

import com.mogujie.mst.hbase.HbaseTestBase;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static com.mogujie.mst.util.HbaseTestUtil.getStrOfRepeatedChar;

/**
 * Created by fenqi on 16/6/18.
 */
public class MultiRowRangeHbaseTest extends HbaseTestBase {
    private static final Logger log = LoggerFactory.getLogger(MultiRowRangeHbaseTest.class);
    private static char seperator = '-', repeatedTimes = 10;
    private static long wantedId = 1;

    @BeforeClass
    public static void beforeClass() {
        log.info("change keyGenerator ...");
        repeatedTimes = 10;
        keyGenerator = (seeds) ->
                (System.currentTimeMillis() + String.valueOf(seperator) + seeds[0]);
        truncateTable = false;
        dropTable = true;

        for (int i = 0; i < repeatedTimes; i++) {
            HbaseTestBase.beforeClass();
        }
    }

    @Test
    public void testMultiRowRangeFilter() {
        int len = String.valueOf(System.currentTimeMillis()).length();
        String startPrefix = getStrOfRepeatedChar(len, '0'),
                endPrefix = getStrOfRepeatedChar(len, '9');

        String startRow = startPrefix + String.valueOf(seperator) + wantedId,
                endRow = endPrefix + String.valueOf(seperator) + wantedId;
        RowRange rowRange = new RowRange(startRow, true, endRow, true);

        List<RowRange> rowRangeList = new ArrayList<>();
        rowRangeList.add(rowRange);

        Filter multiRowRangeFilter = null;

        try {
            multiRowRangeFilter = new MultiRowRangeFilter(rowRangeList);
        } catch (IOException e) {
            Assert.fail(e.toString());
        }

        scanAndCheck(multiRowRangeFilter, repeatedTimes * count);
    }

    @Test
    public void testRowFilterRegexComparator() {
        int len = String.valueOf(System.currentTimeMillis()).length();
        String expr = "^[0-9]{" + len + "}" + String.valueOf(seperator) + wantedId + "$";

        // just kidding... not rely on flag at all.. use 0
        int flag = Pattern.CASE_INSENSITIVE | Pattern.DOTALL;
        RegexStringComparator.EngineType engineType = RegexStringComparator.EngineType.JAVA;

        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new RegexStringComparator(expr, flag, engineType));

        scanAndCheck(rowFilter, repeatedTimes);
    }

}
