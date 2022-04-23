package com.zy.chapter02;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String,Integer>> dataStream = env.addSource(new MySource())
                .flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        for (String word : value.split(" ")){
                            out.collect(new Tuple2<>(word,1));
                        }
                    }
                }).keyBy(0).sum(1)
                ;
        dataStream.print();
        System.out.println(env.getExecutionPlan());
        env.execute();
    }
}
