package com.zy.chapter07;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

public class CustomerSink implements SinkFunction<Tuple2<String,Integer>>, CheckpointedFunction {
    private int thresold;
    private List<Tuple2<String,Integer>> bufferElements;
    private ListState<Tuple2<String,Integer>> checkpointState;

    public CustomerSink(int i){
        this.thresold = i;
        this.bufferElements = new ArrayList<>();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        checkpointState.clear();
        for (Tuple2<String,Integer> ele : bufferElements){
            checkpointState.add(ele);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        ListStateDescriptor<Tuple2<String,Integer>> descriptor = new ListStateDescriptor<Tuple2<String, Integer>>("operator",
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {})
                );
        checkpointState = functionInitializationContext.getOperatorStateStore().getListState(descriptor);
        if(functionInitializationContext.isRestored()){
            for (Tuple2<String,Integer> ele : checkpointState.get()){
                bufferElements.add(ele);
            }
        }
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        bufferElements.add(value);
        if(bufferElements.size() == thresold){
            System.out.println("自定义格式：" + bufferElements);
            bufferElements.clear();
        }
    }
}
