package com.zy.chapter05;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyConnectDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStream<Tuple1<String>> source1 = env.fromElements(
          Tuple1.of("Honda"),
          Tuple1.of("Crown")
        );

        DataStream<Tuple2<String,Integer>> source2 = env.fromElements(
          Tuple2.of("BMW",35),
          Tuple2.of("Tesla",40)
        );

        ConnectedStreams<Tuple1<String>,Tuple2<String,Integer>> connected = source1.connect(source2);
        connected.getFirstInput().print();
        connected.getSecondInput().print();
        env.execute();
    }
}
