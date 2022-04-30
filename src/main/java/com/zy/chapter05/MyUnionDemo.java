package com.zy.chapter05;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyUnionDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStream<Tuple2<String,Integer>> source1 = env.fromElements(
          Tuple2.of("Honda",15),
          Tuple2.of("Crown",25)
        );

        DataStream<Tuple2<String,Integer>> source2 = env.fromElements(
                Tuple2.of("BMW",35),
                Tuple2.of("Tesla",55)
        );

        DataStream<Tuple2<String,Integer>> source3 = env.fromElements(
                Tuple2.of("Ronwei",17),
                Tuple2.of("BYD",45)
        );
        source1.union(source2,source3).print();
        env.execute();
    }
}
