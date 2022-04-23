package com.zy.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.security.UnresolvedPermission;

public class BatchWordCount {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = env.fromElements("Flink batch","batch demo","demo");
        DataSet<Tuple2<String,Integer>> ds= text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String word : value.split(" ")){
                    out.collect(new Tuple2<>(word,1));
                }
            }
        }).groupBy(0).sum(1);

        ds.print();

//        env.execute();

    }
}
