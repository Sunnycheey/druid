package org.apache.druid.client.cache;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ORFCacheConfig {
    @JsonProperty
    private int cacheSize = 400;

    public int getCacheSize() {
        return cacheSize;
    }

    public int getCacheItem() {
        return cacheItem;
    }

    public int getTrainDataThreshold() {
        return trainDataThreshold;
    }

    public int getMaxItem() {
        return maxItem;
    }

    public int getItemNum() {
        return itemNum;
    }

    public int getNumScaled() {
        return numScaled;
    }

    public double getBypassThreashold() {
        return bypassThreashold;
    }

    public void setBypassThreashold(double bypassThreashold) {
        this.bypassThreashold = bypassThreashold;
    }

    public double getCacheThreshold() {
        return cacheThreshold;
    }

    public void setCacheThreshold(double cacheThreshold) {
        this.cacheThreshold = cacheThreshold;
    }

    public int getEvaluateThreshold() {
        return evaluateThreshold;
    }

    @JsonProperty
    private int cacheItem = 100;
    @JsonProperty
    private int trainDataThreshold=4;
    @JsonProperty
    private int maxItem=10;
    @JsonProperty
    private int itemNum=10;
    @JsonProperty
    private int numScaled=5;
    @JsonProperty
    private double bypassThreashold=0.5;
    @JsonProperty
    private double cacheThreshold=0.5;
    @JsonProperty
    private int evaluateThreshold=300;
}
