package com.zy.chapter07;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReductionDemo {
    public static void main(String[] args)throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String,Long>> input = env.fromElements(
          Tuple2.of("BMW",2L),
          Tuple2.of("BMW",2L),
          Tuple2.of("Tesla",3L),
          Tuple2.of("Tesla",4L)
        );
        DataStream<Tuple2<String,Long>> output = input.keyBy(0).countWindow(2).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return Tuple2.of(value1.f0,value1.f1 + value2.f1);
            }
        });
        output.print();
        env.execute();
    }
}
