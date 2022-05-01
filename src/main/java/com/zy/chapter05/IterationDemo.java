package com.zy.chapter05;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class IterationDemo {
    public static void main(String[] args)throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Long> input = env.generateSequence(0,4);
        IterativeStream<Long> iterativeStream = input.iterate();
        DataStream<Long> zero = iterativeStream.map(new RichMapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                return value - 1;
            }
        });
        DataStream<Long> stillGreaterThanZero = zero.filter(new RichFilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value > 0;
            }
        });
        DataStream<Long> end = iterativeStream.closeWith(stillGreaterThanZero);
        zero.print("zero");
        end.print("end");
        env.execute();
    }
}
