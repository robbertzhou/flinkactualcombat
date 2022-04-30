package com.zy.chapter05;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple3;

public class AggregateGroupedTupleDataSet {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<Integer,String,Double>> input = env.fromElements(
                Tuple3.of(1,"a",1.0),
                Tuple3.of(2,"b",2.0),
                Tuple3.of(4,"b",4.0),
                Tuple3.of(3,"c",3.0)
        );
        DataSet<Tuple3<Integer,String,Double>> ds = input.groupBy(1).aggregate(Aggregations.SUM,0).and(Aggregations.MIN,2);
        ds.print();
    }
}
