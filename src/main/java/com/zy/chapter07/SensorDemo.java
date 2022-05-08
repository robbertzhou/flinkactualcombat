package com.zy.chapter07;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SensorDemo {
    public static void main(String[] args) throws Exception{
//前面常规操作，建立环境建立连接装换数据 分组
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> hadoop102 = env.socketTextStream("192.168.0.117", 9999);
        DataStream<SensorReading> sensorData = hadoop102.map(new RichMapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0],new Long(split[1]),new Double(split[2]));
            }
        });
        sensorData.keyBy("id").flatMap(new MyKeyCountMapper(10.0)).print();
        env.execute();
    }


    public static class MyKeyCountMapper extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {
        private double threshold;

        private ValueState<Double> lastTemplature;

        public MyKeyCountMapper(double threshold){
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTemplature = getRuntimeContext().getState(new ValueStateDescriptor<Double>("tmplate",Double.class));
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            Double lastTemp = lastTemplature.value();
            if(lastTemp!=null){
                double diff = Math.abs(value.getTemplature() - lastTemp);
                if(diff > threshold){
                    out.collect(Tuple3.of(value.getId(),lastTemp,value.getTemplature()));
                }
            }

            lastTemplature.update(value.getTemplature());
        }
    }
}


