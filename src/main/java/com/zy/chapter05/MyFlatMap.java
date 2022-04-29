package com.zy.chapter05;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

public class MyFlatMap {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> input = env.fromElements("apache flink is streaming application.");
        DataSet<String> flats = input.flatMap(new RichFlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                for (String str : value.split(" ")){
                    out.collect(str);
                }
            }
        });
        flats.print();
    }
}
