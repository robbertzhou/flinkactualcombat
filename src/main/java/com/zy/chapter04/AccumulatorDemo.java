package com.zy.chapter04;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

public class AccumulatorDemo {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> ds = env.fromElements("Tesla","BMW","Rowei");
        DataSet<String> maped = ds.map(new RichMapFunction<String, String>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("myAccumulator",intCounter);
            }
            IntCounter intCounter = new IntCounter();
            @Override
            public String map(String value) throws Exception {
                intCounter.add(1);
                return value;
            }
        });
        maped.writeAsText("d:\\file.txt");
        JobExecutionResult result = env.execute("ss");
        Object obj = result.getAccumulatorResult("myAccumulator");
        System.out.println("执行后的accumulator is：" + obj);
    }
}
