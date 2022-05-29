package org.apache.druid.client.cache;

public class ORFCacheConfig {
    private final int cacheSize = 400;

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

    private final int cacheItem = 100;
    private final int trainDataThreshold=4;
    private final int maxItem=10;
    private final int itemNum=10;
    private final int numScaled=5;
    private double bypassThreashold=0.5;
    private double cacheThreshold=0.5;
    private final int evaluateThreshold=300;
}
