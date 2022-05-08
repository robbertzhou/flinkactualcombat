package com.zy.chapter07;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AggregateFunctionDemo {
    public static void main(String[] args)throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Tuple2<String,Long>> input = env.fromElements(
                Tuple2.of("BMW",2L),
                Tuple2.of("BMW",2L),
                Tuple2.of("Tesla",3L),
                Tuple2.of("Tesla",4L)
        );

        DataStream<Double> output = input.keyBy(0).countWindow(2).aggregate(new AggregateFunction<Tuple2<String, Long>, Tuple2<Long,Long>, Double>() {
            @Override
            public Tuple2<Long, Long> createAccumulator() {
                return new Tuple2<>(0L,0L);
            }

            @Override
            public Tuple2<Long, Long> add(Tuple2<String, Long> value, Tuple2<Long, Long> accumulator) {
                return new Tuple2(value.f1+accumulator.f0,1L + accumulator.f1);  //f0表示数量
            }

            @Override
            public Double getResult(Tuple2<Long, Long> accumulator) {
                return accumulator.f0 / (double)accumulator.f1;
            }

            @Override
            public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                return new Tuple2<>(a.f0+ b.f0,a.f1 + b.f1);
            }
        });
        output.print();
        env.execute();
    }
}
