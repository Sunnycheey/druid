package org.apache.druid.client.cache.ORF.online.randomforest;




import org.apache.druid.client.cache.ORF.Config;
import org.apache.druid.client.cache.ORF.Utilties;
import org.apache.druid.client.cache.ORF.online.Classifier;
import org.apache.druid.client.cache.ORF.structure.Result;
import org.apache.druid.client.cache.ORF.structure.Sample;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class RandomForest implements Classifier {
    private static final Logger log =new Logger(RandomForest.class);
    private Config config;
    private RandomTree[] trees;
    private int[]        counter;
    private int[][]      treeStats;
    private final int updatedWeight = 2;

    private int numTrees;
    private final AtomicLong insertionPosNum = new AtomicLong(0);
    private final AtomicLong insertionNegNum = new AtomicLong(0);
    private final AtomicLong insertionEvlateNum = new AtomicLong(0);
    private final AtomicLong posNum = new AtomicLong(0);
    private final AtomicLong negNum = new AtomicLong(0);
    private final AtomicLong evlateNum = new AtomicLong(0);

    private ExecutorService executorService;
    private  CountDownLatch countDownLatch;

    public double getPoissonLambdaNeg() {
        return poissonLambdaNeg;
    }

    public void setPoissonLambdaNeg(double poissonLambdaNeg) {
        this.poissonLambdaNeg = poissonLambdaNeg;
    }

    private double poissonLambdaNeg;

    public int getNumTrees(){
        return numTrees;
    }

    public RandomTree getRandomTree(int i){
        return trees[i];
    }
    public RandomForest(Config config, int numClasses, int numFeatures,
                        double[] minFeatRange, double[] maxFeatRange, int[] featureRange) {
        this.config = config;

        // init structure
        this.numTrees = this.config.numTree;
        this.trees = new RandomTree[numTrees];
        this.counter = new int[numClasses];
        this.treeStats = new int[numTrees][numClasses];

        // create trees
        for (int nTree = 0; nTree < numTrees; nTree++) {
            RandomTree tree = new RandomTree(config, numClasses, numFeatures, minFeatRange, maxFeatRange, featureRange);
            this.trees[nTree] = tree;
        }

        executorService = new ThreadPoolExecutor(numTrees, numTrees,1L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(numTrees), Executors.defaultThreadFactory());
        //executorService = Executors.newFixedThreadPool(numTrees);
    }


    public void updateInsertion(List<Sample> samples){
        countDownLatch = new CountDownLatch(numTrees);
        //boolean[] treeErrors = new boolean[this.config.numTree];
        //ExecutorService executorService = Executors.newFixedThreadPool(numTrees);
        for (int nTree = 0; nTree < this.config.numTree; nTree++) {
            int finalNTree = nTree;
            executorService.execute(()->{
                for(Sample sample: samples){
                    Result treeResult = new Result();
                    int numTries;
                    numTries = Utilties.poisson(1.0);

                    // bagging based on poisson distribution, sample is ignored if numTries is 0
                    if (numTries != 0) {
                        for (int nTry = 0; nTry < numTries; nTry++) {
                            this.trees[finalNTree].update(sample);
                        }
                    } else {    // generate stats
                        this.trees[finalNTree].eval(sample, treeResult);
                        int predicted = Utilties.getMaxCoeffIndex(treeResult.getConfidence());
                        this.counter[predicted]++;
                        if (predicted != sample.getLabel()) {
                            this.treeStats[finalNTree][predicted]++;
                            //treeErrors[finalNTree]=true;
                            sample.setWeight(updatedWeight);
                            this.trees[finalNTree].update(sample);
                        }else{
                            //treeErrors[finalNTree]=false;
                        }
                    }
                }
                countDownLatch.countDown();
            });

        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void close(){
        executorService.shutdown();
    }

    public void updateEvict(List<Sample> samples) {
        countDownLatch = new CountDownLatch(numTrees);
        //boolean[] treeErrors = new boolean[this.config.numTree];
        //ExecutorService executorService = Executors.newFixedThreadPool(numTrees);
        for (int nTree = 0; nTree < this.config.numTree; nTree++) {
            int finalNTree = nTree;
            /**
             * 出现.RejectedExecutionException异常，线程池已经被销毁
             * 但后续又调用了被销毁的线程池
             * */
            executorService.execute(()-> {
                for(Sample sample:samples){
                    Result treeResult = new Result();
                    int numTries;
                    numTries = Utilties.poisson(1.0);

                    if (numTries != 0) {
                        for (int nTry = 0; nTry < numTries; nTry++) {
                            this.trees[finalNTree].update(sample);
                        }
                    } else {    // generate stats
                        this.trees[finalNTree].eval(sample, treeResult);
                        int predicted = Utilties.getMaxCoeffIndex(treeResult.getConfidence());
                        this.counter[predicted]++;

                        if (predicted != sample.getLabel()) {
                            this.treeStats[finalNTree][predicted]++;
                            //treeErrors[finalNTree] = true;
                            sample.setWeight(updatedWeight);
                            this.trees[finalNTree].update(sample);
                        } else {
                            //treeErrors[finalNTree] = false;
                        }
                    }
                }
                countDownLatch.countDown();
            });
        }


        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void update(Sample sample) {

    }

    @Override
    public void eval(Sample sample, Result result) {
        for (int nTree = 0; nTree < this.config.numTree; nTree++) {
            Result treeResult = new Result();
            this.trees[nTree].eval(sample, treeResult);

            // weight individual results
            if(this.config.weightIndividualResult) {
                double[] weights = Utilties.calcWeights(this.counter, this.treeStats[nTree]);
                result.addConfidence(treeResult.getConfidence(), weights);
            } else {
                // sum up individual results
                result.addConfidence(treeResult.getConfidence());
            }
        }
        // determine result
        result.divideConfidenceByInteger(this.config.numTree);
        int pre = Utilties.getMaxCoeffIndex(result.getConfidence());
        result.setPrediction(pre);
    }
}
