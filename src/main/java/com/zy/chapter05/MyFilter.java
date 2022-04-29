package com.zy.chapter05;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyFilter {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> input = env.fromElements(-1,-2,-3,1,2,417);
        input.filter(new RichFilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value > 0;
            }
        }).print();

    }
}
