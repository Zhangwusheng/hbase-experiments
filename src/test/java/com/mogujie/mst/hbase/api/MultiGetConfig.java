package com.mogujie.mst.hbase.api;

/**
 * Created by fenqi on 16/6/21.
 */
public class MultiGetConfig {
    private String zookeeperURI;
    private String keysFile;
    private int threadNum;
    private boolean needInsert;
    private String tableName;
    private String columnFamily;
    private String qualifier;
    private int timeout;

    public int getWaitTimeSecs() {
        return waitTimeSecs;
    }

    public void setWaitTimeSecs(int waitTimeSecs) {
        this.waitTimeSecs = waitTimeSecs;
    }

    private int waitTimeSecs;

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public boolean isNeedInteract() {
        return needInteract;
    }

    public void setNeedInteract(boolean needInteract) {
        this.needInteract = needInteract;
    }

    private boolean needInteract;

    public int getSampleNum() {
        return sampleNum;
    }

    public void setSampleNum(int sampleNum) {
        this.sampleNum = sampleNum;
    }

    private int sampleNum;

    public boolean isNeedInsert() {
        return needInsert;
    }

    public void setNeedInsert(boolean needInsert) {
        this.needInsert = needInsert;
    }

    public int getThreadNum() {
        return threadNum;
    }

    public void setThreadNum(int threadNum) {
        this.threadNum = threadNum;
    }

    public String getZookeeperURI() {
        return zookeeperURI;
    }

    public void setZookeeperURI(String zookeeperURI) {
        this.zookeeperURI = zookeeperURI;
    }

    public String getKeysFile() {
        return keysFile;
    }

    public void setKeysFile(String keysFile) {
        this.keysFile = keysFile;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    public String getQualifier() {
        return qualifier;
    }

    public void setQualifier(String qualifier) {
        this.qualifier = qualifier;
    }
}
