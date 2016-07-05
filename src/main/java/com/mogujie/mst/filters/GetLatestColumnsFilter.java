package com.mogujie.mst.filters;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.TimestampsFilter;

import java.util.ArrayList;

/**
 * Created by fenqi on 16/7/5.
 */
public class GetLatestColumnsFilter extends TimestampsFilter {
    private static final Log log = LogFactory.getLog(GetLatestColumnsFilter.class);
    private long max;

    public GetLatestColumnsFilter() {
        super(new ArrayList<>());
        max = -1;
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) {
        if (-1 == max) {
            max = Long.valueOf(v.getTimestamp());
        } else if (max != Long.valueOf(v.getTimestamp())) {
            return ReturnCode.SKIP;
        }
        return ReturnCode.INCLUDE;
    }

    public static GetLatestColumnsFilter parseFrom(byte[] pbBytes) throws DeserializationException {
        return new GetLatestColumnsFilter();
    }

}
