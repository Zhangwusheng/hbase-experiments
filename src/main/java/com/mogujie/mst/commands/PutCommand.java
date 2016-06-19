package com.mogujie.mst.commands;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by fenqi on 16/6/19.
 */
public class PutCommand implements HbaseCommand {
    private String rowKey, columnFamily, qualifier, value;

    public PutCommand(String... args) {
        rowKey = args[0];
        columnFamily = args[1];
        qualifier = args[2];
        value = args[3];
    }

    @Override
    public void execute(Table table) throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
        table.put(put);
    }
}
