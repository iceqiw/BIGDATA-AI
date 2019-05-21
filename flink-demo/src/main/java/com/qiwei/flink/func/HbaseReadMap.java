package com.qiwei.flink.func;

import com.qiwei.flink.client.HbaseClient;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;

/**
 * @author qiwei
 * @description HbaseReadMap
 * @date 2019/5/21 21:42
 */
public class HbaseReadMap extends RichMapFunction<String, String> {

    private HbaseClient client;
    private Table table;

    @Override
    public void open(Configuration parameters) throws Exception {
        client = HbaseClient.getInstance(parameters);
        table = client.getTable(TableName.valueOf("test"));
    }

    @Override
    public void close() throws Exception {
        client.close();
        table.close();
    }

    @Override
    public String map(String s) throws Exception {
        return client.getStringByKey(table, s, "cf", "ok")==null?"00":client.getStringByKey(table, s, "cf", "ok");
    }
}
