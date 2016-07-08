package com.mogujie.mst.hbase.commands;

import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * Created by fenqi on 16/6/18.
 */
public interface HbaseCommand {
    void execute(Table table) throws IOException;
}
