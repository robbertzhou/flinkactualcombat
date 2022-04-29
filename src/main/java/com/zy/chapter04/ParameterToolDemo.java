package com.zy.chapter04;

import org.apache.flink.api.java.utils.ParameterTool;

import java.util.HashMap;
import java.util.Map;

public class ParameterToolDemo {
    public static void main(String[] args) {
        Map properties = new HashMap();
        properties.put("topi","myTopic");
        ParameterTool tool = ParameterTool.fromMap(properties);
        ParameterTool.fromArgs(args);
    }
}
