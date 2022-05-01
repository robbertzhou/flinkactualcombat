package com.zy.chapter05;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

public class BulkIterationDemo {
    public static void main(String[] args)throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        IterativeDataSet<Integer> iterativeDataSet = env.fromElements(0).iterate(10);
        //trans
        DataSet<Integer> maped = iterativeDataSet.map(new RichMapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                double x = Math.random();
                double y = Math.random();
                return value + ((x*x + y* y) < 1?1:0);
            }
        });
        DataSet<Integer> count = iterativeDataSet.closeWith(maped);
        count.map(new RichMapFunction<Integer, Double>() {
            @Override
            public Double map(Integer value) throws Exception {
                return value / (double)10 * 4;
            }
        }).print();
    }
}
