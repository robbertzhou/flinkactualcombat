package com.zy.chapter05;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyReduceDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
    }
}
