package com.zy.chapter07;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

public class WindowAndWatermarkDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> input = env.addSource(new TimeSource());
        DataStream<Tuple2<String,Long>> texts = input.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] splits = value.split(",");
                return Tuple2.of(splits[0],Long.parseLong(splits[1]));
            }
        });
        DataStream<Tuple2<String,Long>> watermark = texts.assignTimestampsAndWatermarks(new WatermarkStrategy<Tuple2<String, Long>>() {
            @Override
            public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<Tuple2<String,Long>>(){
                    private long maxTimestamp;
                    private long delay = 3000;
                    @Override
                    public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
                        maxTimestamp = Math.max(maxTimestamp,event.f1);
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        output.emitWatermark(new Watermark(maxTimestamp + delay));
                    }
                };
            }
        });
        watermark.process(new ProcessFunction<Tuple2<String, Long>, Object>() {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            @Override
            public void processElement(Tuple2<String, Long> stringLongTuple2, ProcessFunction<Tuple2<String, Long>, Object>.Context context,
                                       Collector<Object> collector) throws Exception {
                Long w = context.timerService().currentWatermark();
                System.out.println("水位线：" + w + ",水位线时间：" + sdf.format(w) + ",消息的事件时间："+sdf.format(stringLongTuple2.f1));
            }
        });
        watermark.print();
        env.execute();
    }
}
