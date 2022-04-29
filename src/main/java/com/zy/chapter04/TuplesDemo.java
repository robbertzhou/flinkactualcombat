package com.zy.chapter04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;

public class TuplesDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple1<String>> source = env.fromElements(
           Tuple1.of("Tesla"),
           Tuple1.of("BMW"),
           Tuple1.of("Rowei")
        );
        source.map(new Tuple1Map()).print();
    }

    public static class Tuple1Map implements MapFunction<Tuple1<String>,String>{

        @Override
        public String map(Tuple1<String> value) throws Exception {
            return value.f0;
        }
    }
}
