package com.mogujie.mst.commands;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;

/**
 * Created by fenqi on 16/6/19.
 */
public class ScanCommand implements HbaseCommand {
    private Filter filter = null;
    public ScanCommand(Filter filter) {
        this.filter = filter;
    }
    @Override
    public void execute(Table table) throws IOException {
        Scan scan = new Scan();
        scan.setFilter(this.filter);

        ResultScanner scanner = table.getScanner(scan);
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            System.out.println("Found row : " + result);
        }

        scanner.close();
    }
}