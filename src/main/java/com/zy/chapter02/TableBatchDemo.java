package com.zy.chapter02;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class TableBatchDemo {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
        DataSet<MyOrder> input = env.fromElements(
          new MyOrder(1L,"BMW",1),
          new MyOrder(2L,"Tesla",8),
          new MyOrder(2L,"Tesla",8),
          new MyOrder(3L,"Rolls-Royce",20)
        );
        Table table = tEnv.fromDataSet(input);
        table.printSchema();
        Table filter = table.where($("amount").isGreaterOrEqual(8));
        DataSet<MyOrder> result = tEnv.toDataSet(filter,MyOrder.class);
        result.print();

    }
}
