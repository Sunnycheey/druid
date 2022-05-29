package org.apache.druid.client.cache.ORF;

/*
public class Config {
    public int     numTree                = 10;         // 100
    public int     maxDepth               = 20;
    public double  sampleThreshold        = 300;
    public int     numRandomTests         = 20;
    public int     numEpochs              = 10;
    public boolean refineThreshold        = false;       // true=needs more memory
    public int     leafCacheSize          = 1000;        // only considered if refineThreshold is true  每个节点最多存储这么多样本
    public int     genRanThres            = 50;       //在获取得分时，随机生成这么多次 threshold
    public String  scoreMeasure           = "GINI";     // INFO; GINI
    public double  poissonLambda          = 1.0d;       // do not change this value
    public boolean weightIndividualResult = false;
}
*/


public class Config {
    public int     numTree                = 10;         // 100
    public int     maxDepth               = 10;
    /**
     * 不是删除整棵树，而是修正树，  在某个节点重新计算bestFeature，孩子节点就不要了
     *
     * 下面这个参数也需要调整下
     * */
    public double  sampleThreshold        = 100;//每个节点的样本数超过这个值才能分裂  这个值越大，越抑制分裂？
    public int     numRandomTests         = 30;
    public int     numEpochs              = 1;
    public boolean refineThreshold        = false;       // true=needs more memory
    /**
     * 下面这个参数过大会造成内存耗光，
     * 这是为每棵树的每个节点存了一个数组，下面的参数是数组的长度
     * */
    public int     leafCacheSize          = 150;        // only considered if refineThreshold is true  每个节点最多存储这么多样本
    public int     genRanThres            = 30;       //在获取得分时，随机生成这么多次 threshold
    public String  scoreMeasure           = "GINI";     // INFO; GINI
    public double  poissonLambda          = 1d;       // do not change this value
    //public double  poissonLambdaNeg          = 0.01d;       // do not change this value
    public boolean weightIndividualResult = false;
}


