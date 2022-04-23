package com.zy.chapter02;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MySource extends RichSourceFunction<String> {
    private boolean isRun = true;
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (isRun){
            //word stream
            List<String> stringList = new ArrayList<>();
            stringList.add("one service");
            stringList.add("two dogs");
            stringList.add("three jacks");
            stringList.add("four waters");
            stringList.add("five rice");
            stringList.add("six factories");
            int i = new Random().nextInt(stringList.size());
            sourceContext.collect(stringList.get(i));
            Thread.sleep(500);
        }
    }

    @Override
    public void cancel() {
        isRun = false;
    }
}
