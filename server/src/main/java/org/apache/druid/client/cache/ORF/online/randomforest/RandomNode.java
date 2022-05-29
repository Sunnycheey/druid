package org.apache.druid.client.cache.ORF.online.randomforest;

import org.apache.druid.client.cache.ORF.Config;

import org.apache.druid.client.cache.ORF.Utilties;
import org.apache.druid.client.cache.ORF.structure.Result;
import org.apache.druid.client.cache.ORF.structure.Sample;
import org.apache.druid.java.util.common.logger.Logger;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class RandomNode {
    private Config config;
    private int          numClasses;
    private int          depth;
    public int          label;
    public boolean      isLeaf;
    public double       counter;
    public double       parentCounter;
    public double[]     labelStats;
    private double[]     minFeatRange;
    private double[]     maxFeatRange;
    private int[]  featureRange;
    public RandomNode   leftChildNode;
    public RandomNode   rightChildNode;
    private RandomTest[] randomTests;
    private RandomTest   bestTest;
    private int bestTestIndex;
    private double bestScore;
    private ReentrantReadWriteLock rwLock;
    /**
     * 为了保证多线程下的安全，每个节点加一个读写锁
     * evaluate会获得该节点的读锁
     * update会获取该节点的写锁
     *
     * 获得孩子节点的锁后，会释放当前节点的锁，因为只涉及到节点的分裂，不涉及合并等操作，所以比较简单
     *
     * TODO 要证明加锁的正确性马，并且思考是否有更细粒度的加锁方式
     *
     * 如果不加锁，就认为冲突情况很少，处理下空值的情况？？？
     * */

    private static final Logger log = new Logger(RandomNode.class);

    RandomNode(Config config, int numClasses, int numFeatures,
               double[] minFeatRange, double[] maxFeatRange, int depth, int[] featureRange) {
        // Layout layout = new PatternLayout("%-d{yyyy-MM-dd HH:mm:ss}  [ %l:%r ms ] - [ %p ]  %m%n");
        // Appender appender = new FileAppender(layout,"G://druid_code/insertion module/ORFCache/logs/test.log");
        // log.addAppender(appender);
        this.numClasses = numClasses;
        this.depth = depth;
        this.isLeaf = true;
        this.config = config;
        this.label = -1;
        this.counter = 0.0d;
        this.parentCounter = 0.0d;
        this.labelStats = new double[numClasses];
        this.minFeatRange = minFeatRange;
        this.maxFeatRange = maxFeatRange;
        this.featureRange = featureRange;
        this.bestTestIndex = -1;
        rwLock = new ReentrantReadWriteLock();

        // create random tests
        this.randomTests = new RandomTest[config.numRandomTests];
        for (int nTest = 0; nTest < config.numRandomTests; nTest++) {
            RandomTest randomTest = new RandomTest(config, numClasses, numFeatures, minFeatRange, maxFeatRange);
            this.randomTests[nTest] = randomTest;
        }
    }

    private RandomNode(Config config, int numClasses, int numFeatures,
                       double[] minFeatRange, double[] maxFeatRange, int depth,
                       double[] parentStats, int[] featureRange) {
        this.numClasses = numClasses;
        this.depth = depth;
        this.isLeaf = true;
        this.config = config;
        //this.label = -1;
        this.counter = 0.0d;
        this.parentCounter = Utilties.getSum(parentStats);
        this.labelStats = parentStats;
        this.minFeatRange = minFeatRange;
        this.maxFeatRange = maxFeatRange;
        this.featureRange = featureRange;
        this.bestTestIndex = -1;
        rwLock = new ReentrantReadWriteLock();

        this.label = Utilties.getMaxCoeffIndex(this.labelStats);

        // create random tests
        this.randomTests = new RandomTest[config.numRandomTests];
        for (int nTest = 0; nTest < config.numRandomTests; nTest++) {
            this.randomTests[nTest] = new RandomTest(config, numClasses, numFeatures, minFeatRange, maxFeatRange);
        }
    }

    public RandomTest getBestTest(){
        return bestTest;
    }

    void update(Sample sample) {
        this.counter += sample.getWeight();
        this.labelStats[sample.getLabel()] += sample.getWeight();

        /**
         * update时尝试获取写锁  叶子节点需要加锁，非叶子节点不加锁
         * */


        if (this.isLeaf) {
            for (RandomTest randomTest : this.randomTests) {
                randomTest.update(sample);  // update stats
            }

            this.label = Utilties.getMaxCoeffIndex(this.labelStats);
            rwLock.writeLock().lock();
            if (this.shouldISplit()) {  // not pure and more than 200 samples
                this.isLeaf = false;

                int    nTest    = 0;
                int    minIndex = 0;
                double minScore = 1;

                // select best random test
                for (RandomTest randomTest : this.randomTests) {
                    nTest += 1;
                    double score = randomTest.score();

                    //得分越小越好
                    if (score < minScore) {
                        minScore = score;
                        minIndex = nTest;
                    }
                }

                // delete everything expect best test  找到最合适进行分裂的那个feature 这棵树是一个二叉树
                this.bestTest = this.randomTests[minIndex - 1];
                //this.randomTests = null;
                this.bestTestIndex = minIndex-1;
                this.bestScore = minScore;

                double[][] parentStats = this.bestTest.getStats();  // 0 = trueStats // 1 = falseStats

                int featureId = this.bestTest.getFeatureId();
                double featureThreshold = this.bestTest.getThreshold();

                /**
                 * if featureRange=true 有范围时候  -1
                 * 右孩子大于 featureThreshold 更新minFeatRange=featureThreshold
                 * 左孩子小于 featureThreshold 更新maxFeatRange=featureThreshold
                 *
                 * if featureRange=false 无范围时  >0
                 * 右孩子大于 featureThreshold 更新minFeatRange=featureThreshold   maxFeatRange=this.maxFeatRange+this.maxFeatRange
                 * 左孩子小于 featureThreshold 更新maxFeatRange=featureThreshold
                 * */

                double[] tmpMinFeatRange = new double[this.minFeatRange.length];
                double[] tmpMaxFeatRange = new double[this.maxFeatRange.length];

                //右孩子
                for(int m=0;m<this.minFeatRange.length;m++){
                    if(this.featureRange[m]==-1){
                        if(m==featureId){
                            tmpMinFeatRange[m] = featureThreshold;
                        }else{//不是split feature就不更新
                            tmpMinFeatRange[m] = this.minFeatRange[m];
                        }
                       tmpMaxFeatRange[m] = this.maxFeatRange[m];
                    }else{
                        if(m==featureId){
                            tmpMinFeatRange[m]=featureThreshold;
                            //每次增加一个步长
                            tmpMaxFeatRange[m] = this.maxFeatRange[m]+this.featureRange[m];
                        }else{
                            tmpMinFeatRange[m] = this.minFeatRange[m];
                            tmpMaxFeatRange[m] = this.maxFeatRange[m];
                        }

                    }
                }

                // create child nodes
                this.rightChildNode = new RandomNode(this.config, this.numClasses, this.minFeatRange.length,
                        tmpMinFeatRange, tmpMaxFeatRange, (this.depth + 1), parentStats[0], this.featureRange);

                //左孩子
                for(int m=0;m<this.minFeatRange.length;m++){
                    if(m==featureId){
                        tmpMaxFeatRange[m] = featureThreshold;
                    }else{//不是split feature就不更新
                        tmpMaxFeatRange[m] = this.minFeatRange[m];
                    }
                    tmpMinFeatRange[m] = this.minFeatRange[m];
                }

                this.leftChildNode = new RandomNode(this.config, this.numClasses, this.minFeatRange.length,
                        tmpMinFeatRange, tmpMaxFeatRange, (this.depth + 1), parentStats[1], this.featureRange);
            }
            rwLock.writeLock().unlock();

            /*if(this.bestTest!=null){
                log.error("splitFeatureID:"+this.bestTest.getFeatureId()+" threshhold:"+this.bestTest.getThreshold()+"  trueStats:"
                        + Arrays.toString(this.bestTest.getStats()[0])+" falseStats:"+Arrays.toString(this.bestTest.getStats()[1]));
            }*/

        } else {
            //rwLock.writeLock().unlock();
            /**
             * counter不会丢失，因为从根节点开始递归，这个sample会传到孩子节点，所以
             * counter+parentCounter就是这个叶子节点真实拥有的sample数量
             * */

            /**
             * 第二种 delete tree的方法  重新生成bestTest
             * 如果重新生成的比 bestTest得分高就替换   可能需要对近期的data 提高权重
             *
             * 非叶子节点都会计算一下 bestTest  TODO
             * */
            for (RandomTest randomTest : this.randomTests) {
                randomTest.update(sample);  // update stats
            }
            int    nTest    = 0;
            int    minIndex = 0;
            double minScore = 1;
            if (this.bestTest.eval(sample)) {
                this.rightChildNode.update(sample);
            } else {
                this.leftChildNode.update(sample);
            }

            // select best random test   TODO ORF优化点之一：动态删除树
            for (RandomTest randomTest : this.randomTests) {
                nTest += 1;
                double score = randomTest.score();

                //得分越小越好
                if (score < minScore) {
                    minScore = score;
                    minIndex = nTest;
                }
            }
            //TODO trade-off
            /*if(this.bestScore-minScore>2){
                this.isLeaf=true;
                update(sample);
            }*/
            if (minIndex-1 != this.bestTestIndex){
                this.isLeaf=true;
                update(sample);
            }

        }
    }

    void eval(Sample sample, Result result) {
        if (this.isLeaf) {  // classify sample
            //rwLock.readLock().unlock();
            if ((this.counter + this.parentCounter) != 0) {
                result.setConfidence(this.labelStats.clone());
                result.divideConfidenceByDouble(this.counter + this.parentCounter);
                result.setPrediction(this.label);
            } else {    // there is no information
                double[] conf = new double[this.labelStats.length];
                for (int i = 0; i < conf.length; i++) {
                    conf[i] = 1.0d / ((double) this.numClasses);
                }
                result.setConfidence(conf);
                result.setPrediction(0);
            }
        } else {    // search corresponding leaf
            /*if(this.bestTest==null){
                System.out.println("bestTest is null error");
                System.exit(-1);
            }*/
            /**
             * 代码走到这里，train线程要么isLeaf==true
             * 要么已经分裂完毕，isLeaf=false
             *      this.bestTest
             *      this.rightChildNode
             *      this.leftChildNode 均不为空
             * */
            rwLock.readLock().lock();
            rwLock.readLock().unlock();
            if (this.bestTest.eval(sample)) {
                /*if(this.rightChildNode==null){
                    System.out.println("right child node is null error");
                    System.exit(-1);
                }*/
                this.rightChildNode.eval(sample, result);
            } else {
                /*if(this.leftChildNode==null){
                    System.out.println("left child node is null error");
                    System.exit(-1);
                }*/
                this.leftChildNode.eval(sample, result);
            }
        }
    }

    private boolean shouldISplit() {
        boolean isPure = false;

        // check if leaf is pure
        for (int nClass = 0; nClass < this.numClasses; nClass++) {
            /**
             * 这说明所有的数据都分到了一边， 那这个节点肯定就是叶子节点，就没必要再分裂了
             * */
            if (this.labelStats[nClass] == (this.counter + this.parentCounter)) {
                isPure = true;
                break;
            }
        }

        // returns true if leaf is not pure, has seen more than 'threshold' values, and is not to deep
        return !(isPure || this.depth >= this.config.maxDepth || this.counter < this.config.sampleThreshold);
    }
}
