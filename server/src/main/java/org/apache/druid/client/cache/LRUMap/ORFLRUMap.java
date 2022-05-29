package org.apache.druid.client.cache.LRUMap;


import org.apache.druid.client.cache.DataSeq.DataSeq;
import org.apache.druid.client.cache.Item.ItemEntry;
import org.apache.druid.client.cache.Item.ValueItem;
import org.apache.druid.client.cache.ORF.Utilties;
import org.apache.druid.client.cache.ORF.online.randomforest.RandomForest;
import org.apache.druid.client.cache.ORF.structure.Result;
import org.apache.druid.client.cache.ORF.structure.Sample;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * key是字符串，
 * cache size只算item数量
 * */
public class ORFLRUMap extends LinkedHashMap<String, ValueItem> {
    private static final Logger log = new Logger(ORFLRUMap.class);

    //在cache中的item的最大rrip值，超过这个值必须移除cache
    private final int maxRRIP = 10;
    //cache size
    private final int sizeInBytes;

    private double bypassThreshold;
    private final List<String> keysToRemove;
    /**
     * 一些统计信息
     * TODO 这些统计信息有的可能没用
     * */
    private final AtomicLong numBytes;//当前的cache size（同样也是item数量）
    private final AtomicLong evictCount;//从cache中移除的数量
    private final AtomicLong orfEvictNums;//使用ORF模型预测移除cache的数量
    private final AtomicLong orfBypassNums;//put的总次数，如果没有insertion模型，就是cache miss的次数

    public void setDataSeq(DataSeq dataSeq) {
        this.dataSeq = dataSeq;
    }
    private DataSeq dataSeq;

    public void setTrainDataThreshold(int trainDataThreshold) {
        this.trainDataThreshold = trainDataThreshold;
    }
    private int trainDataThreshold;

    /**
     * 记录着cache中达到最大RRIP值的item
     * */
    private HashMap<String, Integer> maxRRIPItem;

    public void setRfEvict(RandomForest rfEvict) {
        this.rfEvict = rfEvict;
    }
    public void setRfInsertion(RandomForest rfInsertion) {
        this.rfInsertion = rfInsertion;
    }
    public void setIsrfEvict(boolean isrfEvict) {
        this.isrfEvict = isrfEvict;
    }
    public void setIsrfInsertion(boolean isrfInsertion) {
        this.isrfInsertion = isrfInsertion;
    }
    public void setMaxItem(int maxItem) {
        this.maxItem = maxItem;
    }
    public void setItemNum(int itemNum) {
        this.itemNum = itemNum;
    }
    public void setNumScaled(int numScaled) {
        this.numScaled = numScaled;
    }

    private RandomForest rfEvict;
    private RandomForest rfInsertion;
    /**
     * 以下Boolean值用来判断是否可以使用ORF模型
     * 当模型样本数超过一定阈值后才可以使用ORF模型进行预测
     * */
    boolean isrfEvict;
    boolean isrfInsertion;
    /**
     * 以下三个参数是否可以合起来，以减少调参个数  TODO  这三个参数对结果影响不大
     * */
    private int maxItem;
    private int itemNum;
    private int numScaled;

    public int getArrCacheSize() {
        return arrCacheSize;
    }

    public void setArrCacheSize(int arrCacheSize) {
        this.arrCacheSize = arrCacheSize;
    }

    private int arrCacheSize;

    public void setCacheScaledThreshold(double cacheScaledThreshold) {
        this.cacheScaledThreshold = cacheScaledThreshold;
    }
    //cache中item所占比例超过这个值 才能达到bypass的条件
    private double cacheScaledThreshold;

    public ORFLRUMap(
            final int sizeInBytes,//cache size 也就是cache中的最大item数量
            final double bypassThreshold //预测item bypass的最大阈值，即超过这个阈值，item不移进cache
    ) {
        //父类构造函数的第一个参数没啥用
        super(sizeInBytes,0.75f,true);
        this.sizeInBytes = sizeInBytes;
        this.bypassThreshold = bypassThreshold;

        numBytes = new AtomicLong(0);
        evictCount = new AtomicLong(0);
        orfEvictNums = new AtomicLong(0);
        orfBypassNums = new AtomicLong(0);

        keysToRemove = new ArrayList<>();
        maxRRIPItem = new HashMap<>();
    }

    public long getNumBytes()
    {
        return numBytes.get();
    }
    public long getEvictionCount()
    {
        return evictCount.get();
    }
    public long getOrfEvictionNums(){return orfEvictNums.get();}
    public long getOrfBypassNums(){return orfBypassNums.get();}


    /**
     * ORFCache类调用该方法
     * 如果命中，要更新LRU、LFU、index等值
     *
     * @return*/
    @Override
    public ValueItem get(Object key){
        ValueItem value = super.get(key);
        if(value != null){
            //super.remove(key);
            /**
             * TODO 测试下
             * 如果cache hit,更新LFU
             * */
            value.setItemEntryLFU(value.getItemEntry().getLfu()+1);
            super.put((String) key,value);
        }
        return value;
     }

    private Result onlineEvaluate(Sample sample, RandomForest rf){
        Result result = new Result();
        rf.eval(sample, result);
        return result;
    }

    public double[] genFeaturesPastData(String key){
        ItemEntry oneKey = dataSeq.getDataSeqIndexMapValue(key);
        int curPastIndex = dataSeq.getDataCurIndex();
        int dataSeqDataSize =dataSeq.getDataSeqDataSize();
        return internalGenFeatures(oneKey, dataSeqDataSize, curPastIndex);
    }

    public double[] internalGenFeatures(ItemEntry oneKey, int seqSize, int curIndex){
        double[] res = new double[3];
        /**
         * 不可能是空，windowData中的这个key已经提前放入pastDataSeqArr中，
         * 是空就报错
         * */
        if(oneKey==null){
            res[0]=0;
            res[1]=res[2]=seqSize;
            return res;
        }else{
            int itemNumInScope=oneKey.getPastNum();
            int oneKeySize = oneKey.getIndexListSize();
            if(itemNumInScope!=oneKeySize){
                System.out.println("ORFLRUMap itemNumInScope must equal to IndexList size  " +
                        "ItemNumInScope:"+itemNumInScope+"  ListSize:"+oneKeySize);
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
                    res[2]=arrCacheSize-(firstIndex-curIndex);
                }
                if(curIndex>lastIndex){
                    res[1]=curIndex-lastIndex;
                }else{
                    res[1]=arrCacheSize-(lastIndex-curIndex);
                }
            }
            res[0] = (res[0])/((double)(seqSize))*itemNum*numScaled;
            res[1] = (res[1])/((double) (seqSize))*itemNum;
            res[2] = (res[2])/((double) (seqSize))*itemNum;
            return res;
        }
    }

    /**
     * ORF模型使用该方法进行put
     * 该方法只留下key和value
     * 其他的参数都用set get方法进行设置
     * */
    public ValueItem putORF(String key, ValueItem value){
        //totalPutNums  这个值应该和miss count一样吧
        if(!isrfEvict){
            //直接使用LRU的策略
            return this.put(key,value);
        }else{
            boolean canBypass = false;
            double newRRIP = 0.0;//RRIP值越大，说明被移除cache的概率越大
            boolean predictBypass= false;
            //Random rand = new Random();

            if(isrfInsertion){
                //说明可以使用ORF的insertion模型
                /**
                 * 如果过去一段时间没有访问，就直接bypass
                 *
                 * dataSeq 中不包含key这个数据，这是在put完成后再放入dataSeq中的
                 * */
                ItemEntry oneKey = dataSeq.getDataSeqIndexMapValue(key);
                //if(oneKey==null && rand.nextInt(100)>30){
                //if(oneKey==null && oneKey.getPastNum()<=1){
                if(oneKey==null){
                    canBypass=true;
                }else {
                    if(oneKey.getPastNum()==0){
                        System.out.println("can not be 0");
                        System.exit(-1);
                    }
                    //predictBypass=false;
                    //TODO 去掉insertion ??
                    orfBypassNums.incrementAndGet();
                    double[] insertFeatures = genFeaturesPastData(key);
                    Sample sampleInsertion = new Sample(new double[]{insertFeatures[0], insertFeatures[1], insertFeatures[2]}, 0, 1.0, 0);
                    Result resultInsertion = onlineEvaluate(sampleInsertion, rfInsertion);
                    int predInsert = Utilties.getMaxCoeffIndex(resultInsertion.getConfidence());

                    if (resultInsertion.getConfidence()[1] > bypassThreshold && (numBytes.get() > cacheScaledThreshold * sizeInBytes)) {
                        canBypass = true;
                    } else if (predInsert == 1) {
                        predictBypass = true;
                        newRRIP = maxRRIP * (resultInsertion.getConfidence()[1]);
                    } else {
                        predictBypass = false;
                    }
                }
            }
            if(canBypass){
                //不需要插入 do nothing
                //bypassCount.incrementAndGet();
            }else{
                //需要插入
                if(numBytes.get()>=sizeInBytes){
                    //cache满
                    if(maxRRIPItem.size()>0){
                        Iterator<String> iterator = maxRRIPItem.keySet().iterator();
                        /**
                         * 不能删的太多 也不能超过 numBytes.get() > cacheScaledThreshold*sizeInBytes
                         * 这样才能保证maxRRIPItem在下一次调用putORF的时候还有值，要不然每次都删光，和不加RRIP这个机制没有区别了。
                         * 因为程序永远不会跳到这里
                         * */
                        keysToRemove.clear();
                        while (iterator.hasNext() && (numBytes.get()>=(cacheScaledThreshold*sizeInBytes))){
                            String toRemoveKey = (String) iterator.next();
                            //从Cache中删除item
                            remove(toRemoveKey);
                            keysToRemove.add(toRemoveKey);
                            evictCount.incrementAndGet();
                        }
                        for(String delKey:keysToRemove){
                            maxRRIPItem.remove(delKey);
                        }
                        keysToRemove.clear();
                        //如果在这里每次使用一次maxRRIPItem，最后都把它clear掉，效果会不会好点？
                    }else{
                        /**
                         * 跳到这里，说明cache中没有达到最大RRIP值的，所以需要ORF进行预测
                         * 这时需要预测cache中所有的item，只需要更新RRIP，而不用真正地移除
                         *
                         * 到达这里，说明maxRRIPItem已经是空的了
                         * */
                        int entryIndex = 0;
                        Iterator<Map.Entry<String, ValueItem>> it = entrySet().iterator();
                        while(it.hasNext()){
                            Map.Entry<String, ValueItem> next = it.next();
                            double lruFeature = (((double)entryIndex)/((double)this.size()))*maxItem;
                            double lfuFeature = (((double)next.getValue().getItemEntry().getLfu())/((double) this.size()))*maxItem;
                            double[] evictFeatures = genFeaturesPastData(next.getKey());
                            /**
                             * cache中可能存在很早之前的item，所以pastDataSeqIndexMap.get(key)会返回空，此时要处理下
                             * */
                            Sample sample = new Sample(new double[]{lruFeature,lfuFeature,evictFeatures[0],evictFeatures[1],evictFeatures[2]},0,1.0,0);
                            entryIndex++;
                            Result result = onlineEvaluate(sample, rfEvict);
                            int pred = Utilties.getMaxCoeffIndex(result.getConfidence());
                            int newRRIPTmp = (int) (result.getConfidence()[1]*maxRRIP+next.getValue().getItemEntry().getRrip());
                            //int newRRIPTmp = (3+next.getValue().getItemEntry().getRrip());
                            /**
                             * 这些值应该加入maxRRIPItem中，留作下一次移除
                             * */
                            orfEvictNums.incrementAndGet();
                            if(pred==1 && (newRRIPTmp>=maxRRIP)){
                                maxRRIPItem.put(next.getKey(),newRRIPTmp);
                            }else{
                                ItemEntry itemEntry = next.getValue().getItemEntry();
                                if(pred==1) {
                                    itemEntry.setRrip(newRRIPTmp);
                                }else{
                                    // TODO RRIP是每次减一个固定的值，还是按比例缩放
                                    int tmNewRRIP=Math.max(1,(int) (next.getValue().getItemEntry().getRrip()-result.getConfidence()[0]*maxRRIP));
                                    //int tmNewRRIP=Math.max(1,(int) (next.getValue().getRrip()-4));
                                    //itemEntry.setRrip(tmNewRRIP);
                                }
                                /**
                                 * TODO 测试下， 不加这句话有问题吗？
                                 * */
                                //next.getValue().setItemEntry(itemEntry);
                            }
                        }
                    }
                }
                /**
                 * 如果此时cache还是满的，就使用LRU策略从cache中移除一个item
                 * */
                if(numBytes.get()>=sizeInBytes){
                    return this.put(key, value);
                }
                if(isrfInsertion){
                    if(predictBypass){
                        /**
                         * TODO 测试下
                         * */
                        value.getItemEntry().setRrip((int) newRRIP);
                    }else {
                        value.getItemEntry().setRrip(1);
                    }
                }else{
                    value.getItemEntry().setRrip(1);
                }
                //numBytes.incrementAndGet();
                numBytes.addAndGet(value.getItemEntry().getValueSize());
                ValueItem old = super.put(key, value);//插入新数据
                if(old!=null){
                    numBytes.addAndGet(-old.getItemEntry().getValueSize());
                }
                return old;
            }
            return value;
        }
    }

    /**
     * LRU可以直接使用这几个方法
     * */
    @Override
    public ValueItem put(String key, ValueItem value){
        //numBytes.incrementAndGet();//向cache中put 一项
        numBytes.addAndGet(value.getItemEntry().getValueSize());
        Iterator<Map.Entry<String, ValueItem>> it = entrySet().iterator();

        long totalEvictSize = 0L;
        keysToRemove.clear();
        //删除LRU stack里面的数据，使得cache中有地方可以put new item
        while (numBytes.get()-totalEvictSize>sizeInBytes && it.hasNext()){
            evictCount.incrementAndGet();
            Map.Entry<String, ValueItem> next = it.next();
            //改之前hitRate巨高？  没用
            //totalEvictSize+=1;
            totalEvictSize+=next.getValue().getItemEntry().getValueSize();
            keysToRemove.add(next.getKey());
        }

        for(String keyToRemove: keysToRemove){
            remove(keyToRemove);
        }
        keysToRemove.clear();

        ValueItem old = super.put(key, value);
        /**
         * 说明cache中有这个item（虽然这在我们的逻辑中不存在）
         * */
        if(old!=null){
            numBytes.addAndGet(-old.getItemEntry().getValueSize());
        }
        return old;
    }

    @Override
    public ValueItem remove(Object key)
    {
        ValueItem value = super.remove(key);
        if (value != null) {
            numBytes.addAndGet(-value.getItemEntry().getValueSize());
        }
        return value;
    }

    /**
     * Don't allow key removal using the underlying keySet iterator
     * All removal operations must use ByteCountingLRUMap.remove()
     */
    @Override
    public Set<String> keySet()
    {
        return Collections.unmodifiableSet(super.keySet());
    }

    @Override
    public void clear()
    {
        numBytes.set(0L);
        evictCount.set(0L);
        orfEvictNums.set(0L);
        orfBypassNums.set(0L);
        super.clear();
    }
}