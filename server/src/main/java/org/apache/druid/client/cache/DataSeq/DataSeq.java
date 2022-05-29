package org.apache.druid.client.cache.DataSeq;


import org.apache.druid.client.cache.Item.ItemEntry;

import java.util.HashMap;

public class DataSeq {



    /**
     * DataSeq这个类的对象 有下列成员:
     *
     * */
    private String[] dataSeqArr;//key的数组
    private HashMap<String, ItemEntry> dataSeqIndexMap;
    private int dataCurIndex;
    private int dataSeqDataSize;
    private final int arrSize;



    /**
     * 往DataSeq中插入一条数据
     * TODO 测试正确性
     * */
    public void updateAuxiliaryArr(ItemEntry value) {
        String key = value.getKey();
        dataSeqDataSize++;
        ItemEntry updateKey = dataSeqIndexMap.get(key);
        if(updateKey==null){
            value.setPastNum(1);
            /**
             * 为啥这里会不一致呢？？ pastNum!=indexListSize
             * */
            value.clearIndexList();
            value.indexListAddLast(dataCurIndex);
            if(value.getPastNum() != value.getIndexListSize()){
                System.out.println("null updateAuxiliaryArr something error");
            }
            dataSeqIndexMap.put(key,value);
        }else{
            //value=updateKey;//去掉这个拷贝是否合理？？
            updateKey.setPastNum(updateKey.getPastNum()+1);
            updateKey.indexListAddLast(dataCurIndex);
            if(updateKey.getPastNum() != updateKey.getIndexListSize()){
                System.out.println("not null updateAuxiliaryArr something error");
            }
            dataSeqIndexMap.put(key,updateKey);
        }

        /**
         * 要覆盖的index的地方有值
         * */
        if(dataSeqArr[dataCurIndex]!=null){
            deleteOneItemFromDataSeq(dataCurIndex);
        }
        dataSeqArr[dataCurIndex]=key;
        dataCurIndex++;
        //dataCurIndex=(dataCurIndex==(trainDataThreshold+1)*cacheSize)?0:index;
        dataCurIndex=(dataCurIndex==arrSize)?0:dataCurIndex;
    }

    public ItemEntry getDataSeqIndexMapValue(String key){
        return dataSeqIndexMap.get(key);
    }

    public int getDataSeqDataSize(){
        return dataSeqDataSize;
    }

    public int getDataCurIndex(){
        return dataCurIndex;
    }

    /**
     * 从该dataSeq中删除index指向的旧数据
     *
     * TODO 测试下，调用该函数是否会对外部对象有影响
     * */
    public void deleteOneItemFromDataSeq(int index){
        String degradeKey = dataSeqArr[index];
        ItemEntry degradeItem = dataSeqIndexMap.get(degradeKey);
        /**
         * hashmap中没有这个key，或者对应的index列表是空，或者index最老的数据索引与index不匹配--->直接覆盖旧的数据即可
         * */
        if(degradeItem==null || degradeItem.getIndexListSize()==0 || degradeItem.getIndexListFirstItem()!=index){
            //do nothin 说明pastDataCurIndex上的这个item是没用的
        }else{
            if(degradeItem.getPastNum() != degradeItem.getIndexListSize()){
                System.out.println(this.getClass()+"deleteOneItemFromPastDataSeq pastNum not equal to indexList size");
                System.exit(-1);
            }
            if(degradeItem.getPastNum()==1){
                dataSeqIndexMap.remove(degradeKey);
            }else{
                degradeItem.setPastNum(degradeItem.getPastNum()-1);
                if(degradeItem.indexListDeleteFirst()!=index){
                    System.out.println(this.getClass()+"deleteOneItemFromPastDataSeq dataCurIndex != HashMap.getFirst  error");
                    System.exit(-1);
                }
                dataSeqIndexMap.put(degradeKey, degradeItem);
            }
            dataSeqDataSize--;
        }
    }

    public DataSeq(int arrSize){

        dataSeqArr = new String[arrSize];
        dataCurIndex=0;
        dataSeqIndexMap = new HashMap<>();
        dataSeqDataSize=0;
        this.arrSize=arrSize;
    }

    public void close(){
        dataCurIndex=0;
        dataSeqIndexMap.clear();
        dataSeqDataSize=0;
    }
}
