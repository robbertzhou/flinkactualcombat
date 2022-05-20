package com.zy.chapter07;

import com.zy.chapter02.MySource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideoutDemo {
    public static void main(String[] args) throws Exception {
        OutputTag<String> sideOut1 = new OutputTag<String>("side-output"){};
        OutputTag<String> sideOut2 = new OutputTag<String>("side-output1"){};
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> input = env.addSource(new MySource());
        SingleOutputStreamOperator mainStream = input.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
                collector.collect(s);
                context.output(sideOut1,s + "11");
                context.output(sideOut2,s + "22");
            }
        });
        mainStream.print();
        mainStream.getSideOutput(sideOut1).print("111");
        mainStream.getSideOutput(sideOut2).print("222");
        env.execute();
    }
}
