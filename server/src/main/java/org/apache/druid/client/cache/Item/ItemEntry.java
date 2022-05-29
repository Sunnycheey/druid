package org.apache.druid.client.cache.Item;

import java.util.LinkedList;
import java.util.Objects;

/**
 * 不考虑value Size
 * 每个item所维护的meta data:
 * key:数据的key
 * index: 数据在data sequence中的索引
 * lfu: 在cache中的数据的访问次数
 * rrip: 在cache中的数据的rrip值
 * */
public class ItemEntry{
    public ItemEntry(String key, int lfu,int rrip, int valueSize) {
        this.key = key;
        this.lfu = lfu;
        this.rrip = rrip;
        /**
         * TODO 不使用indexList是否能将其置空？ =null
         * */
        this.indexList = new LinkedList<>();
        this.valueSize = valueSize;
        this.pastNum=0;
    }

    /*public ItemEntry(String key, int lfu, int rrip, int pastNum,int valueSize){
        this.key = key;
        this.lfu = lfu;
        this.rrip = rrip;
        this.pastNum = pastNum;
        indexList = new LinkedList<>();
        this.valueSize=valueSize;
    }*/

    public LinkedList<Integer> getIndexList() {
        return indexList;
    }

    public int getPastNum() {
        return pastNum;
    }

    public void setPastNum(int pastNum) {
        this.pastNum = pastNum;
    }

    public void indexListAddLast(int index){
        this.indexList.addLast(index);
    }


    public int indexListDeleteFirst(){
        return this.indexList.removeFirst().intValue();
    }

    public int getIndexListSize(){
        return this.indexList.size();
    }

    public int getIndexListFirstItem(){
        return this.indexList.getFirst().intValue();
    }

    public int getIndexListLastItem(){
        return this.indexList.getLast().intValue();
    }

    public void clearIndexList(){
        this.indexList.clear();
    }

    // lfu 用两个字节   pastNum/rrip用一个字节就可以
    private int lfu;
    private int pastNum;
    private int rrip;
    private String key;
    private int valueSize=1;

    public int getValueSize() {
        return valueSize;
    }

    public void setValueSize(int valueSize) {
        this.valueSize = valueSize;
    }

    private LinkedList<Integer> indexList;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getLfu() {
        return lfu;
    }

    public void setLfu(int lfu) {
        this.lfu = lfu;
    }

    public int getRrip() {
        return rrip;
    }

    public void setRrip(int rrip) {
        this.rrip = rrip;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return key.equals(((ItemEntry) o).key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }
}
