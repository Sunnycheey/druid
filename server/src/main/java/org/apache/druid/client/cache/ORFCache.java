package org.apache.druid.client.cache;

import org.apache.druid.client.cache.DataSeq.DataSeq;
import org.apache.druid.client.cache.DataSeq.FutureDataSeq;
import org.apache.druid.client.cache.DataSeq.OPTMap;
import org.apache.druid.client.cache.Item.ItemEntry;
import org.apache.druid.client.cache.Item.ValueItem;
import org.apache.druid.client.cache.LRUMap.ORFLRUMap;
import org.apache.druid.client.cache.ORF.Config;
import org.apache.druid.client.cache.ORF.online.randomforest.RandomForest;
import org.apache.druid.client.cache.ORF.structure.Sample;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class ORFCache implements Cache{
    private static final Logger log = new Logger(ORFCache.class);

    private final ORFCacheConfig config = new ORFCacheConfig();

    private final ORFLRUMap orflruMap;

    private final Object clearLock = new Object();

    public FutureDataSeq getFutureDataSeq(){
        return futureDataSeq;
    }
    //将来一段时间的访问序列
    private FutureDataSeq futureDataSeq;
    //过去一段时间的访问序列
    private DataSeq pastDataSeq;
    //inference时 过去一段时间的访问序列
    private DataSeq inferenceDataSeq;

    public DataSeq getPastDataSeq(){
        return pastDataSeq;
    }


    public void setTrainDataThreshold(int trainDataThreshold) {
        this.trainDataThreshold = trainDataThreshold;
    }
    private int trainDataThreshold;

    /**
     * 只要get==null   missCount++   统计hit rate和miss rate
     * */
    private final AtomicLong hitCount = new AtomicLong(0);
    private final AtomicLong missCount = new AtomicLong(0);

    /**
     * 用于生成训练数据的 最优cache   使用Belady-MIN算法生成
     * */
    private OPTMap optMap;
    public OPTMap getOptMap(){
        return optMap;
    }
    /**
     * 存放训练数据的队列，使用生产者-消费者的多线程模式
     * **/
    private BlockingQueue<Sample> trainDataQue;
    private BlockingQueue<Sample> trainDataQueInsertion;

    public BlockingQueue<Sample> getTrainDataQue(){
        return trainDataQue;
    }

    public BlockingQueue<Sample> getTrainDataQueInsertion(){
        return trainDataQueInsertion;
    }
    /**
     * 存放数据序列，用于放入OPTMap，模拟最优Cache的过程，用于生成训练数据
     * */
    private BlockingQueue<ItemEntry> dataSequences;

    /**
     * online random forest的evict和insert的两个模型
     * 以及模型对应的配置项，两个模型使用同一个配置
     * TODO: 两个模型是否使用不同的配置项
     * */
    private RandomForest rf;
    private RandomForest rfInsertion;
    Config configORF = new Config();

    /**
     * 喂给模型的训练样本数，用来判断是否可以用ORF模型来进行inference
     * */
    private int totalTrainData = 0;
    private int totalTrainDataInsertion = 0;
    /**
     * 标记是否使用online random forest进行换出
     * */
    private boolean isORF = false;
    private boolean isORFInsertion = false;
    /**
     * 可以使用ORF进行inference的阈值样本数，和上面两个变量一起使用
     * */
    private final int evaluateThresholdEvict;
    private final int evaluateThresholdInsert;

    /**
     * 生成feature时的一些归一化参数，验证这些参数对结果影响不大
     * */
    private final int maxItem;
    private final int itemNum;
    private final int numScaled;

    /**
     * 在OPTCache中存放一批删除的item
     * */
    private final LinkedList<ItemEntry> windowData;
    private final List<String> toRemove;


    /**
     * cache大小（cache中item的数量，假设每个item的key-value一样大）
     * */
    private final int cacheSize;
    private final int arrCacheItem;
    private final int cacheItem;

    /**
     维护要移除的hashmap
     */
    private HashSet<String> evictItemList;

    /**
     * 训练线程  evict和insert两个模型各一个线程
     * */
    private TrainThread evictTrainThread;
    private TrainThread insertTrainThread;
    /**
     * 生成训练数据的线程
     * */
    private GenTrainingThread genTrainingThread;
    /**
     * 每次训练一批线程，然后线程休眠一段时间
     * */
    private int TrainBatch = 1000;//每次训练1000个样本
    public void setTrainBatch(int trainBatch) {
        TrainBatch = trainBatch;
    }

    /**
     * Blocking Queue中的数量不能超过这个阈值，超过时，就将多余的旧的数据直接舍弃
     * */
    private int QSizeThreshold=5000;
    public void setQSizeThreshold(int QSizeThreshold) {
        this.QSizeThreshold = QSizeThreshold;
    }

    /**
     * 每次训练完，线程休眠的毫秒数
     * */
    private int sleepTimeMS=10;
    public void setSleepTimeMS(int sleepTimeMS) {
        this.sleepTimeMS = sleepTimeMS;
    }

    /**
     * TODO 计算cache中item的数量，当嵌入Druid时，通过这个函数预估cache中存在的item个数
     * */
    /*private int calCacheItem(){
        return cacheSize;
    }*/



    public ORFCache(int cacheSize, int cacheItem, int trainDataThreshold,int maxItem, int itemNum, int numScaled,
                    double bypassThreashold, double cacheThreshold, int evaluateThreshold){
        this.evictItemList = new HashSet<>();
        this.cacheSize = cacheSize;
        this.maxItem = maxItem;
        this.itemNum = itemNum;
        this.numScaled = numScaled;

        this.evaluateThresholdEvict = evaluateThreshold;
        this.evaluateThresholdInsert = evaluateThreshold;

        this.orflruMap = new ORFLRUMap(cacheSize, bypassThreashold);
        this.orflruMap.setCacheScaledThreshold(cacheThreshold);
        this.orflruMap.setItemNum(itemNum);
        this.orflruMap.setMaxItem(maxItem);
        this.orflruMap.setNumScaled(numScaled);
        this.optMap = new OPTMap(cacheSize);


        windowData = new LinkedList<>();
        toRemove = new ArrayList<>();

        this.trainDataThreshold=trainDataThreshold;
        this.cacheItem = cacheItem;
        arrCacheItem = cacheItem*(this.trainDataThreshold+1);
        futureDataSeq = new FutureDataSeq(arrCacheItem);
        pastDataSeq = new DataSeq(arrCacheItem);
        inferenceDataSeq = new DataSeq(arrCacheItem);


        /**
         * 生成evict和insert的RandomForest对象，虽然比较臃肿，但是无伤大雅
         * */
        double[] minFeatureRangeEvict = new double[5];
        double[] maxFeatureRangeEvict = new double[5];
        int[] featureRangeEvict = new int[5];
        featureRangeEvict[1] = itemNum;//LFU的范围无法确定，而另外四个feature的取值范围是可以确定的
        featureRangeEvict[0] = featureRangeEvict[2] = featureRangeEvict[3] = featureRangeEvict[4] = -1;
        for(int i=0;i<minFeatureRangeEvict.length;i++){
            minFeatureRangeEvict[i]=0;
        }
        maxFeatureRangeEvict[0] =  maxItem;
        maxFeatureRangeEvict[1] = maxItem;
        maxFeatureRangeEvict[2] = maxFeatureRangeEvict[3]=maxFeatureRangeEvict[4]=itemNum;

        rf = new RandomForest(configORF, 2, 5, minFeatureRangeEvict, maxFeatureRangeEvict, featureRangeEvict);
        /**
         * TODO 先不使用正负样本不均衡的处理方式，减少参数
         * */
        //rf.setPoissonLambdaNeg(lambda);

        int[] insertionFeatureRange = new int[3];
        insertionFeatureRange[0] = insertionFeatureRange[1] = insertionFeatureRange[2] = -1;
        double[] insertionMinFeatureRange = new double[3];
        double[] insertionMaxFeatureRange = new double[3];
        for(int i=0;i<insertionMinFeatureRange.length;i++){
            insertionMinFeatureRange[i]=0;
            insertionMaxFeatureRange[i]=itemNum;
        }
        rfInsertion = new RandomForest(configORF, 2, 3, insertionMinFeatureRange, insertionMaxFeatureRange, insertionFeatureRange);

        /**
         * 初始化BlockingQueue，
         * TODO: 有更好的结构吗？ 生产者总是快于消费者，也可以不使用BlockingQueue 不加锁会不会快点  使用原子变量记录queue的长度
         * */
        trainDataQue = new LinkedBlockingQueue<>();
        trainDataQueInsertion = new LinkedBlockingQueue<>();
        dataSequences = new LinkedBlockingDeque<>();

        /**
         * 开启线程
         * */
        this.genTrainingThread = new GenTrainingThread("genTrainingThread");
        this.genTrainingThread.start();

        this.evictTrainThread = new TrainThread("evict");
        this.insertTrainThread = new TrainThread("insert");
        this.evictTrainThread.start();
        this.insertTrainThread.start();

    }


    class GenTrainingThread extends Thread{
        private Thread t;
        private String threadName;
        private volatile boolean isConsumer = true;

        GenTrainingThread(String name){
            threadName = name;
        }
        @Override
        public void run(){
            while (isConsumer){
                /**
                 * TODO 直接在生产者处就 sample？
                 * */
                while (dataSequences.size()>QSizeThreshold){
                    dataSequences.poll();
                }
                int dNum=0;
                while ((!dataSequences.isEmpty())&&dNum<TrainBatch){
                    dNum++;
                    ItemEntry itemEntry = dataSequences.poll();
                    if(itemEntry!=null) {
                        genTrainingData(itemEntry);
                    }
                }
                try {
                    /**
                     * TODO 还有什么更好的采样方法吗？
                     * */
                    Thread.sleep(sleepTimeMS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            /**
             * 不清空会有问题
             * */
            orflruMap.clear();
            optMap.clear();
        }

        public void stopConsumer(){
            isConsumer=false;
        }
        @Override
        public void start () {
            if (t == null) {
                t = new Thread(this, threadName);
                t.start ();
            }
        }
    }

    /**
     * 构造一个训练线程
     * */
    class TrainThread extends Thread {
        private Thread t;
        private String threadName;
        private volatile boolean isConsumer = true;

        TrainThread(String name) {
            threadName = name;
        }

        @Override
        public void run() {
            /**
             * 消费肯定比生成的慢，所以不需要睡眠
             * */
            if(threadName.equals("evict")){
                while (isConsumer){
                    while (trainDataQue.size()>QSizeThreshold){
                        trainDataQue.poll();
                    }
                    LinkedList<Sample> samples = new LinkedList<>();
                    for(int nEpoch = 0; nEpoch<configORF.numEpochs;nEpoch++){
                        int sambpleSize=0;
                        while(!trainDataQue.isEmpty() && sambpleSize<TrainBatch){
                            sambpleSize++;
                            Sample sample = trainDataQue.poll();//抛出一个样本
                            samples.add(sample);
                        }
                    }
                    rf.updateEvict(samples);

                    try {
                        Thread.sleep(sleepTimeMS);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                rf.close();
            }else if(threadName.equals("insert")){
                while (isConsumer){
                    while (trainDataQueInsertion.size()>trainDataThreshold){
                        trainDataQueInsertion.poll();
                    }
                    LinkedList<Sample> samples = new LinkedList<>();
                    for(int nEpoch = 0; nEpoch<configORF.numEpochs;nEpoch++){
                        int sambpleSize=0;
                        while(!trainDataQueInsertion.isEmpty() && sambpleSize<TrainBatch){
                            sambpleSize++;
                            Sample sample = trainDataQueInsertion.poll();//抛出一个样本
                            samples.add(sample);
                        }
                    }
                    rfInsertion.updateInsertion(samples);
                    try {
                        Thread.sleep(sleepTimeMS);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                rfInsertion.close();
            }else{
                System.out.println("threadName error");
            }
        }
        public void stopConsumer(){
            isConsumer=false;
        }
        @Override
        public void start () {
            if (t == null) {
                t = new Thread(this, threadName);
                t.start ();
            }
        }
    }



    /**
     * 在ORFCacheProvider中使用
     * */
    public static ORFCache create(final ORFCacheConfig config) {
        return new ORFCache(config.getCacheSize(),config.getCacheItem(),config.getTrainDataThreshold(),
                config.getMaxItem(),config.getItemNum(),config.getNumScaled(), config.getBypassThreashold(),
                config.getCacheThreshold(),config.getEvaluateThreshold());
    }


    public double[] genFeaturesPastData(String key){
        ItemEntry oneKey = pastDataSeq.getDataSeqIndexMapValue(key);
        int seqSize = pastDataSeq.getDataSeqDataSize();
        int pastCurIndex = pastDataSeq.getDataCurIndex();
        return internalGenFeatures(oneKey, seqSize, pastCurIndex);
    }

    /**
     * TODO 测试下 调用时机
     * */
    public double[] internalGenFeatures(ItemEntry oneKey, int seqSize, int curIndex){
        double[] res = new double[3];

        if(oneKey==null){
            res[0]=0;
            res[1]=res[2]=seqSize;
            return res;
        }else{
            /**
             * Integer 直接==会有问题？？
             * ==与equals的区别？？
             * */
            int itemNumInScope=oneKey.getPastNum();
            int oneKeySize= oneKey.getIndexListSize();
            if(itemNumInScope!=oneKeySize){
                System.out.println("ORFCache itemNumInScope must equal to IndexList size " +
                        "       ItemNumInScope:"+itemNumInScope+"  ListSize:"+oneKeySize);
                System.exit(-1);
            }
            if(itemNumInScope<=1){
                //相当于过去一段时间没有访问这个key 或者 只访问了一次
                res[0]=itemNumInScope;
                /**
                 * TODO 过去一段时间只放问一次 也要把firstIndex和lastIndex设为 pastDataSeqSize吗
                 * */
                res[1]=res[2]=seqSize;
            }else{
                res[0]=itemNumInScope;
                /**
                 * IndexList:
                 * first                               last
                 * x        x x x....x                 x
                 * firstIndex         LastIndex        curIndex
                 * */
                int firstIndex = oneKey.getIndexListFirstItem();
                int lastIndex = oneKey.getIndexListLastItem();
                if(curIndex>firstIndex){
                    res[2]=curIndex-firstIndex;
                }else{
                    /**
                     * TODO 改成trainDataThreshold+1 试试？
                     * */
                    res[2]=arrCacheItem-(firstIndex-curIndex);
                }
                if(curIndex>lastIndex){
                    res[1]=curIndex-lastIndex;
                }else{
                    res[1]=arrCacheItem-(lastIndex-curIndex);
                }
            }
            res[0] = (res[0])/((double)(seqSize))*itemNum*numScaled;
            res[1] = (res[1])/((double) (seqSize))*itemNum;
            res[2] = (res[2])/((double) (seqSize))*itemNum;
            return res;
        }
    }

    private void sequenceGenTraingData(ItemEntry value){
        /**
         * 数据放入队列，供消费者使用
         * */
        dataSequences.offer(value);
        /**
         * 更新dataSeqArrInference
         * 逻辑同genTrainingData
         * */
        ItemEntry inferenceValue = new ItemEntry(value.getKey(), 1, 0, value.getValueSize());
        inferenceDataSeq.updateAuxiliaryArr(inferenceValue);
    }

    public void genTrainingData(ItemEntry value)  {
        int entryIndex;
        futureDataSeq.updateAuxiliaryArr(value, optMap, evictItemList);

        //达到生成训练数据的条件  不可能大于，只能取等号！！！
        if(futureDataSeq.getDataSeqDataSize() >= arrCacheItem){
            if(pastDataSeq.getDataSeqDataSize()>=arrCacheItem){
                entryIndex=0;
                /**
                 * 遍历cache中的数据，生成训练数据集
                 * */
                for(Map.Entry<String, ItemEntry> entry: optMap.entrySet()){
                    /**
                     * 这时候的futureDataSeq与pastDataSeq大小相等
                     * */
                    double lruFeature = (((double)entryIndex)/((double)optMap.size()))*maxItem;
                    double lfuFeature = (((double)entry.getValue().getLfu())/((double) optMap.size()))*maxItem;
                    /**
                     * cache中可能存在很早之前的item，所以pastDataSeqIndexMap.get(key)会返回空，此时要处理下
                     * */
                    //TODO 这里有点问题！！！！   pastDataSeq.getDataCurIndex()能否作为这个item的curIndex
                    double[] evictFeatures = genFeaturesPastData(entry.getKey());

                    if(futureDataSeq.getDataSeqIndexMapValue(entry.getKey())==null){
                        Sample sample = new Sample(new double[]{lruFeature, lfuFeature, evictFeatures[0], evictFeatures[1], evictFeatures[2]}, 1, 1.0, 0);
                        trainDataQue.offer(sample);
                    }else{
                        Sample sample = new Sample(new double[]{lruFeature, lfuFeature, evictFeatures[0], evictFeatures[1], evictFeatures[2]}, 0, 1.0, 0);
                        trainDataQue.offer(sample);
                    }
                    entryIndex++;
                }
            }


            /**
             * 从dataSeq中去掉cacheItem个item
             * updateAuxiliaryArr函数已经对dataCurIndex++了，这时dataCurIndex指向的是dataSeqArr中最老的数据
             * */
            int tmpCurIndex=futureDataSeq.getDataCurIndex();
            while(futureDataSeq.getDataSeqDataSize() > (trainDataThreshold) * cacheItem){
                /**
                 * dataSeqDataSize >= (trainDataThreshold+1)* cacheItem因为有这个条件，所以tmpCurIndex肯定是链表中最老的数据
                 * */
                ItemEntry deleteKV = futureDataSeq.deleteOneItemFromDataSeq(tmpCurIndex,optMap,evictItemList);
                windowData.addLast(deleteKV);
                //从DataSeq中移除这个item，并更新相应的数据结构
                tmpCurIndex++;
                tmpCurIndex=(tmpCurIndex>=arrCacheItem)?0:tmpCurIndex;
            }

            //将windowData中的数据放入cache中，同时生成训练数据
            for(ItemEntry kvS: windowData){
                //没有这个key   下面这个条件是达到生成训练集  将windowData中的item依次放入OPTCache中
                if(pastDataSeq.getDataSeqDataSize()>=(arrCacheItem)){
                    /**
                     * 生成insertion模型的feature   windowData中的数据在最后加入pastData里面
                     * */
                    double[] insertionFeatures = genFeaturesPastData(kvS.getKey());
                    /**
                     * TODO 这样生成数据是否合理  是否要判断cache满了 再生成insertion的训练数据   DONE 满了生成数据会造成数据的imbalance
                     * 这相当于对每个数据都用于生成insertion模型的训练数据
                     * 但是insertion的行为不会影响后续eviction模型生成的训练数据
                     * */
                    /**
                     * TODO PTCacheTest测试结果加了下面这句话后，效果反而不好了 ？？
                     * 在一个window中加入cache中的数据，如果有重复的，那么重复的数据肯定不能bypass掉
                     * 但是实验得出，这样做效果并不好
                     * */
                    if(futureDataSeq.getDataSeqIndexMapValue(kvS.getKey())==null){
                        Sample sample = new Sample(new double[]{insertionFeatures[0],insertionFeatures[1],insertionFeatures[2]}, 1, 1.0, 0);
                        trainDataQueInsertion.offer(sample);
                        continue;
                        //不需要插入
                    }else{
                        Sample sample = new Sample(new double[]{insertionFeatures[0],insertionFeatures[1],insertionFeatures[2]}, 0, 1.0, 0);
                        trainDataQueInsertion.offer(sample);
                    }
                }

                //需要插入，判断是否命中OPT Cache
                ItemEntry cacheValue = optMap.get(kvS.getKey());
                if(cacheValue != null){
                    //命中cache，就更新对应的lru和lfu值
                    //optMap.remove(kvS.getKey());
                    cacheValue.setLfu(cacheValue.getLfu()+1);
                    //optMap.put(kvS.getKey(), cacheValue);
                }else{
                    //cache满了
                    if (optMap.getNumBytes()>=optMap.getSizeInBytes()){
                        /**
                         * 可以不用遍历cache，用一个HashMap记录将来不会出现的item  这样做不行，因为无法以O(1)的时间得到LRU这个feature*
                         * 后续考虑去掉这部分的训练数据，也即从cache中移除的不生产训练数据
                         *
                         * 将来不访问的数据直接全部移除
                         * TODO 每次删除部分数据，保证cache大于 cacheThreshold*cacheItem
                         * 对于OPTCache 是否加这个条件对结果影响不大
                         * */

                        for(String sKey:evictItemList){
                            optMap.remove(sKey);
                        }
                        evictItemList.clear();

                        //如果上述步骤没有将item从cache中移除，也就是cache还是满的   则使用LRU规则进行移除
                        Iterator<Map.Entry<String, ItemEntry>> it = optMap.entrySet().iterator();
                        long totalEvictionSize=0;
                        toRemove.clear();
                        /**
                         * TODO: totalEvictionSize改成valueSize
                         * */
                        while (optMap.getNumBytes()-totalEvictionSize>=optMap.getSizeInBytes()){
                            Map.Entry<String, ItemEntry> next = it.next();
                            toRemove.add(next.getKey());
                            totalEvictionSize+=next.getValue().getValueSize();
                        }
                        for(String keyToRemove: toRemove){
                            optMap.remove(keyToRemove);
                        }
                        toRemove.clear();
                    }
                    //cache现在是未满的
                    //ItemEntry newEntry = new ItemEntry(kvS.getKey(),1,0);
                    kvS.setLfu(1);
                    optMap.put(kvS.getKey(), kvS);
                    if(futureDataSeq.getDataSeqIndexMapValue(kvS.getKey())==null){
                        evictItemList.add(kvS.getKey());
                    }
                }
                //从auxiliaryDataSeq中删除的数据同时加入pastAuxiliaryDataSeq中
                /**
                 * 为啥一定要加下面两句？？
                 * */
                /*if(kvS.getPastNum()!=0 && kvS.getIndexListSize()!=0){
                    System.out.println("pastDataSeq something error");
                    System.exit(-1);
                }*/
                //kvS.clearIndexList();
                //kvS.setPastNum(0);
                pastDataSeq.updateAuxiliaryArr(kvS);
            }
            windowData.clear();

            if(!isORF) {
                totalTrainData += trainDataQue.size();
            }
            if(!isORFInsertion){
                totalTrainDataInsertion += trainDataQueInsertion.size();
            }
        }
    }


    @Nullable
    @Override
    public byte[] get(NamedKey Nkey) {//get方法不需要进行预测
        String key = Nkey.namespace+ Arrays.toString(Nkey.key);
        ValueItem valueItem;
        synchronized (clearLock) {
            valueItem = orflruMap.get(key);
        }
        if (valueItem == null) {
            //这块不用加在同步里面吗？？感觉多线程下会有影响啊
            missCount.incrementAndGet();
            return null;
        } else {
            /**
             * 往一个队列中添加数据，train线程从队列中拿数据进行训练
             * 就是生产者消费者模式
             *
             * train需要维护 两个数据列表：1、过去一段时间的访问数据  2、将来一段时间的访问数据
             * inference需要维护一个数据列表： 过去一段时间的访问数据序列
             * 这三个序列是每次都更新的，但不是每个数据都训练，隔一段时间训练一次？
             * 训练的两个数据列表和inference的数据列表是独立的
             * */
            /**
             * TODO: 测试下 传入一个ItemEntry参数，在函数中改变值，在函数外是否有影响
             * */
            sequenceGenTraingData(valueItem.getItemEntry());
            hitCount.incrementAndGet();
            return valueItem.getValue();
        }
    }

    /**
     *
     * */
    @Override
    public void put(NamedKey Nkey, byte[] value) {
        //TODO 这种方式是否合理？？
        synchronized (clearLock) {
            String key = Nkey.namespace+ Arrays.toString(Nkey.key);

            if(totalTrainData> evaluateThresholdEvict){
                isORF=true;
            }
            if(totalTrainDataInsertion >evaluateThresholdInsert){
                isORFInsertion=true;
            }
            ItemEntry itemEntry = new ItemEntry(key, 1, 0,value.length);

        /**
         * orflruMap set 一些变量
         * */
        /**
         * 先genTrainData 再putORF
         * 要保证auxiliaryDataSeq在putORF之前就有这个item
         *
         * private ItemEntry[] dataSeqArr;
         * private HashMap<String, ItemEntry> dataSeqIndexMap;
         * private int dataCurIndex;
         * private Long dataSeqDataSize;
         * */
            orflruMap.setTrainDataThreshold(trainDataThreshold);
            orflruMap.setDataSeq(inferenceDataSeq);
            orflruMap.setIsrfEvict(isORF);
            orflruMap.setIsrfInsertion(isORFInsertion);
            orflruMap.setRfEvict(rf);
            orflruMap.setRfInsertion(rfInsertion);
            orflruMap.setArrCacheSize(arrCacheItem);
            ValueItem valueItem = new ValueItem(itemEntry, value);
            orflruMap.putORF(key , valueItem);
        /**
         * 注意与putORF的先后关系
         * */
            sequenceGenTraingData(itemEntry);
        }
    }


    @Override
    public Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys) {
        Map<NamedKey, byte[]> retVal = new HashMap<>();
        for (NamedKey key : keys) {
            final byte[] value = get(key);
            if (value != null) {
                retVal.put(key, value);
            }
        }
        return retVal;
    }

    @Override
    public void close(String namespace) {

    }

    @Override
    public CacheStats getStats() {
        return new CacheStats(
                hitCount.get(),
                missCount.get(),
                orflruMap.getEvictionCount(),
                orflruMap.getOrfBypassNums(),
                0,0,0
        );
    }

    /**
     * 表示是本地cache
     * */
    @Override
    public boolean isLocal() {
        return true;
    }

    /**
     * 获取统计信息  TODO
     * */
    @Override
    public void doMonitor(ServiceEmitter emitter) {
        final ServiceMetricEvent.Builder builder = ServiceMetricEvent.builder();
        emitter.emit(builder.build("query/cache/orf/missCount", missCount));
        emitter.emit(builder.build("query/cache/orf/hitCount", hitCount));

    }

    @Override
    public void close() throws IOException {
        trainDataQue.clear();

    }


}
