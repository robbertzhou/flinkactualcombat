package com.zy.chapter02;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class WordCountTableForStream {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        //define all tableenvironment parameters
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv,bsSettings);
        DataStream<String> dataStream = sEnv.addSource(new MySource());
        Table table = tEnv.fromDataStream(dataStream,$("word"));
        Table t1 = table.where($("word").like("%f%"));
        String explain = tEnv.explain(t1);
        System.out.println(explain);
        tEnv.toAppendStream(t1, Row.class).print();
        sEnv.execute();
    }
}
