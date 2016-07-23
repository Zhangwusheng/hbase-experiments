package com.mogujie.mst.hbase.filters;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;

import java.io.IOException;
import java.util.List;

/**
 * Created by fenqi on 16/7/23.
 * This custom filter do one thing:
 * If a row contains a value equal the argument, return the entire row
 * or return nothing
 */
public class CustomFilterForKalisamy extends ValueFilter {
    private boolean hit = false;

    public CustomFilterForKalisamy(CompareOp valueCompareOp, ByteArrayComparable valueComparator) {
        super(valueCompareOp, valueComparator);
        hit = false;
    }

    @Override
    public ReturnCode filterKeyValue(Cell c) {
        if(!this.doCompare(this.compareOp, this.comparator,
                c.getValueArray(), c.getValueOffset(), c.getValueLength())) {
            hit = true;
        }
        return ReturnCode.INCLUDE;
    }

    @Override
    public void filterRowCells(List<Cell> kvs) {
        if (!hit) {
            kvs.clear();
        }
    }

    @Override
    public void reset() {
        hit = false;
    }

    public static CustomFilterForKalisamy parseFrom(byte[] pbBytes) throws DeserializationException {
        org.apache.hadoop.hbase.protobuf.generated.FilterProtos.ValueFilter proto;
        try {
            proto = org.apache.hadoop.hbase.protobuf.generated.FilterProtos.ValueFilter.parseFrom(pbBytes);
        } catch (InvalidProtocolBufferException var5) {
            throw new DeserializationException(var5);
        }

        CompareOp valueCompareOp = CompareOp.valueOf(proto.getCompareFilter().getCompareOp().name());
        ByteArrayComparable valueComparator = null;

        try {
            if(proto.getCompareFilter().hasComparator()) {
                valueComparator = ProtobufUtil.toComparator(proto.getCompareFilter().getComparator());
            }
        } catch (IOException var6) {
            throw new DeserializationException(var6);
        }

        return new CustomFilterForKalisamy(valueCompareOp, valueComparator);
    }

}

