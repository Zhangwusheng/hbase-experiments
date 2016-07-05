package com.mogujie.mst.filters;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by fenqi on 16/6/19.
 */
public class CustomFilterForRishi extends SingleColumnValueFilter {
    private static final Log log = LogFactory.getLog(CustomFilterForRishi.class);
    private static final String SEPARATOR = "#";
    private char splitChar;
    private long startTimestamp;
    private long endTimestamp;

    public CustomFilterForRishi(byte[] family, byte[] qualifier, char c, long start, long end) {
        super(family,
                qualifier,
                CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(
                        Bytes.toBytes(
                                new StringBuilder()
                                        .append(c)
                                        .append(SEPARATOR).append(start)
                                        .append(SEPARATOR).append(end)
                                        .toString())));
        if (SEPARATOR.equals(String.valueOf(c))) {
            throw new RuntimeException("SEPARATOR can not be " + SEPARATOR);
        }
        this.splitChar = c;
        this.startTimestamp = start;
        this.endTimestamp = end;
    }

    protected CustomFilterForRishi(byte[] family, byte[] qualifier, char c, long start, long end,
                                   boolean filterIfMissing, boolean latestVersionOnly) {
        this(family, qualifier, c, start, end);
        this.filterIfMissing = filterIfMissing;
        this.latestVersionOnly = latestVersionOnly;
    }


    @Override
    public ReturnCode filterKeyValue(Cell c) {
        if (this.matchedColumn) {
            return ReturnCode.INCLUDE;
        } else if (this.latestVersionOnly && this.foundColumn) {
            return ReturnCode.NEXT_ROW;
        } else if (!CellUtil.matchingColumn(c, this.columnFamily, this.columnQualifier)) {
            return ReturnCode.INCLUDE;
        } else {
            this.foundColumn = true;
            if (this.filterColumnValue(c.getValueArray(), c.getValueOffset(), c.getValueLength())) {
                return this.latestVersionOnly ? ReturnCode.NEXT_ROW : ReturnCode.INCLUDE;
            } else {
                this.matchedColumn = true;
                return ReturnCode.INCLUDE;
            }
        }
    }

    private boolean filterColumnValue(byte[] data, int offset, int length) {
        String strValue = new String(data, offset, length);
        String[] strFields = strValue.split(String.valueOf(this.splitChar));
        if (2 == strFields.length) {
            try {
                long timestamp = Long.valueOf(strFields[1]);
                return timestamp < this.startTimestamp || timestamp > this.endTimestamp;
            } catch (NumberFormatException e) {
                return true;
            }
        }
        return true;
    }

    public static CustomFilterForRishi parseFrom(byte[] pbBytes) throws DeserializationException {
        org.apache.hadoop.hbase.protobuf.generated.FilterProtos.SingleColumnValueFilter proto;
        try {
            proto = org.apache.hadoop.hbase.protobuf.generated.FilterProtos.SingleColumnValueFilter.parseFrom(pbBytes);
        } catch (InvalidProtocolBufferException var6) {
            throw new DeserializationException(var6);
        }

        CompareFilter.CompareOp compareOp = CompareFilter.CompareOp.valueOf(proto.getCompareOp().name());

        ByteArrayComparable comparator;
        try {
            comparator = ProtobufUtil.toComparator(proto.getComparator());
        } catch (IOException var5) {
            throw new DeserializationException(var5);
        }

        char c;
        long start, end;
        try {
            String value = new String(comparator.getValue());
            String[] strFields = value.split(SEPARATOR);
            if (strFields.length < 3) {
                throw new RuntimeException("value format err");
            }
            c = strFields[0].charAt(0);
            start = Long.valueOf(strFields[1]);
            end = Long.valueOf(strFields[2]);
        } catch (Exception var7) {
            throw new DeserializationException(var7);
        }

        return new CustomFilterForRishi(proto.hasColumnFamily()?proto.getColumnFamily().toByteArray():null,
                proto.hasColumnQualifier()?proto.getColumnQualifier().toByteArray():null,
                c, start, end,
                proto.getFilterIfMissing(), proto.getLatestVersionOnly());
    }
}