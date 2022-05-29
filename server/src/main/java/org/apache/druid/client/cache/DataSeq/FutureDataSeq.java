package org.apache.druid.client.cache.DataSeq;


import org.apache.druid.client.cache.Item.ItemEntry;

import java.util.HashMap;
import java.util.HashSet;

public class FutureDataSeq {
    private ItemEntry[] dataSeqArr;//key的数组
    private HashMap<String, Integer> dataSeqIndexMap;
    private int dataCurIndex;
    private int dataSeqDataSize;
    private final int arrSize;

    public int getDataSeqDataSize(){
        return dataSeqDataSize;
    }

    public int getDataCurIndex(){
        return dataCurIndex;
    }

    public Integer getDataSeqIndexMapValue(String key){
        return dataSeqIndexMap.get(key);
    }

    /**
     * TODO 测试下
     * */
    public ItemEntry deleteOneItemFromDataSeq(int index, OPTMap optMap, HashSet<String> evictItemList){
        ItemEntry degradeKey = dataSeqArr[index];
        Integer degradeNum = dataSeqIndexMap.get(degradeKey.getKey());
        //future data不会对pastNum和indexList进行操作
        if(degradeNum==null){
            //do nothin
            //应该不会到这里
            System.out.println("can not reach here");
            System.exit(-1);
        }else{
            if(degradeNum.intValue()==1){
                dataSeqIndexMap.remove(degradeKey.getKey());
                /**
                 * 这时候需要看optMap中是否有这个key，如果有，就需要把他加入到evictItem中
                 * get方法会将数据插入到最新的位置  所以这里必须使用containsKey方法
                 * */
                if(optMap.containsKey(degradeKey.getKey())){
                    //if(optMap.get(degradeKey.getKey())!=null){
                    //说明这个key将来不再访问
                    evictItemList.add(degradeKey.getKey());
                }
            }else{
                dataSeqIndexMap.put(degradeKey.getKey(), degradeNum-1);
                if(evictItemList.contains(degradeKey.getKey())){
                    evictItemList.remove(degradeKey.getKey());
                }
            }
            //更新dataSeqSize
            dataSeqDataSize--;
        }
        dataSeqArr[index]=null;
        return degradeKey;//删除的数据应该加入pastData里面，所以要返回
    }


    public void updateAuxiliaryArr(ItemEntry value, OPTMap optMap, HashSet<String> evictItemList) {
        dataSeqDataSize++;
        String key = value.getKey();
        Integer updateKeyNum = dataSeqIndexMap.get(key);
        if(updateKeyNum==null){
            dataSeqIndexMap.put(key,1);
        }else{
            dataSeqIndexMap.put(key, updateKeyNum+1);
        }

        if(evictItemList.contains(key)){
            evictItemList.remove(key);
        }

        //加上这句有啥影响吗？
        if(dataSeqArr[dataCurIndex]!=null){
            deleteOneItemFromDataSeq(dataCurIndex, optMap, evictItemList);
        }

        dataSeqArr[dataCurIndex]=value;
        dataCurIndex++;
        dataCurIndex=(dataCurIndex==arrSize)?0:dataCurIndex;
    }

    public ItemEntry deleteOneItemFromDataSeqForAccuracy(int index){
        ItemEntry degradeKey = dataSeqArr[index];
        Integer degradeNum = dataSeqIndexMap.get(degradeKey.getKey());
        if(degradeNum==null){
            System.out.println("can not reach here");
            System.exit(-1);
        }else{
            if(degradeNum.intValue()==1){
                dataSeqIndexMap.remove(degradeKey.getKey());
            }else{
                dataSeqIndexMap.put(degradeKey.getKey(), degradeNum-1);
            }
            //更新dataSeqSize
            dataSeqDataSize--;
        }
        dataSeqArr[index]=null;
        return degradeKey;//删除的数据应该加入pastData里面，所以要返回
    }


    public void updateAuxiliaryArrForAccuracy(ItemEntry value) {
        dataSeqDataSize++;
        String key = value.getKey();
        Integer updateKeyNum = dataSeqIndexMap.get(key);
        if(updateKeyNum==null){
            dataSeqIndexMap.put(key,1);
        }else{
            dataSeqIndexMap.put(key, updateKeyNum+1);
        }
        //加上这句有啥影响吗？
        if(dataSeqArr[dataCurIndex]!=null){
            deleteOneItemFromDataSeqForAccuracy(dataCurIndex);
        }
        dataSeqArr[dataCurIndex]=value;
        dataCurIndex++;
        dataCurIndex=(dataCurIndex==arrSize)?0:dataCurIndex;
    }

    public FutureDataSeq(int arrSize){
        dataSeqArr = new ItemEntry[arrSize];
        dataCurIndex=0;
        dataSeqIndexMap = new HashMap<>();
        dataSeqDataSize=0;
        this.arrSize=arrSize;
    }
}
