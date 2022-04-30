package com.zy.chapter05;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

public class MyMapPartitionDemo {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> lines = env.fromElements("BMW","Tesla","Ronwei");
        lines.mapPartition(new RichMapPartitionFunction<String, Integer>() {
            @Override
            public void mapPartition(Iterable<String> values, Collector<Integer> out) throws Exception {
                Integer i = 0;
                for (String v : values){
                    i++;
                }
                out.collect(i);
            }
        }).print();

    }
}
