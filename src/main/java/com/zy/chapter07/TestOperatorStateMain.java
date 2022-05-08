package com.zy.chapter07;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestOperatorStateMain {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String,Integer>> input = env.fromElements(
                Tuple2.of("spark",3),
                Tuple2.of("flink",4),
                Tuple2.of("hadoop",5),
                Tuple2.of("spark",7)
        );
        input.addSink(new CustomerSink(3)).setParallelism(1);
        env.execute();
    }
}
