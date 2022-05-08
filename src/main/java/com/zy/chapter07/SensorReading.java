package com.zy.chapter07;

public class SensorReading {
    public SensorReading(){}

    public SensorReading(String id,long device,double templature){
        this.id = id;
        this.device = device;
        this.templature = templature;
    }

    private String id;
    private Long device;
    private double templature;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getDevice() {
        return device;
    }

    public void setDevice(Long device) {
        this.device = device;
    }

    public double getTemplature() {
        return templature;
    }

    public void setTemplature(double templature) {
        this.templature = templature;
    }
}
