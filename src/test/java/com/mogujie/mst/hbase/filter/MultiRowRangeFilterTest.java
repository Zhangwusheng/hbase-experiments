package com.mogujie.mst.hbase.filter;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by fenqi on 16/6/18.
 */
public class MultiRowRangeFilterTest extends FilterTestBase {
    private static final Logger log = LoggerFactory.getLogger(MultiRowRangeFilterTest.class);

    @Test
    public void MultiRowRangeFilterTest() {
        try {
            String startRow = keyPrefix + 0, endRow = keyPrefix + 5;
            RowRange rowRange = new RowRange(startRow, false, endRow, false);

            List<RowRange> rowRangeList = new ArrayList<>();
            rowRangeList.add(rowRange);

            Filter multiRowRangeFilter = new MultiRowRangeFilter(rowRangeList);
            scanAndCheck(multiRowRangeFilter, 9);
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }


}
