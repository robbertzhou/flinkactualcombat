package com.zy.chapter02;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class SqlStreamDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        DataStream<String> stream = env.addSource(new MySource());
        Table table = tEnv.fromDataStream(stream,$("word"));
        Table result = tEnv.sqlQuery("select * from " + table + " where word like '%f%'");
        DataStream<Row> rows = tEnv.toAppendStream(result, Row.class);
        rows.print();
        env.execute();
    }
}
