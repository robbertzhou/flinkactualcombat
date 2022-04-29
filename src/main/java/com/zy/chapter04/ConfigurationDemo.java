package com.zy.chapter04;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.dag.IterationNode;

public class ConfigurationDemo {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> input = env.fromElements(1,2,3,5,10,12,15,16);
        Configuration configuration = new Configuration();
        configuration.setInteger("limit",8);

        DataSet<Integer> filter = input.filter(new RichFilterFunction<Integer>() {
            private Integer limit;
            @Override
            public void open(Configuration parameters) throws Exception {
                limit = parameters.getInteger("limit",0);
            }

            @Override
            public boolean filter(Integer value) throws Exception {
                return value > limit;
            }
        }).withParameters(configuration);
        filter.print();
    }
}
