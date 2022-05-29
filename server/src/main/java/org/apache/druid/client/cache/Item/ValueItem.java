package org.apache.druid.client.cache.Item;

public class ValueItem {
    private ItemEntry itemEntry;
    private byte[] value;

    public ItemEntry getItemEntry() {
        return itemEntry;
    }
    public void setItemEntry(ItemEntry itemEntry) {
        this.itemEntry = itemEntry;
    }
    public byte[] getValue() {
        return value;
    }
    public void setValue(byte[] value) {
        this.value = value;
    }

    public void setItemEntryLFU(int lfu){
        this.itemEntry.setLfu(lfu);
    }

    public ValueItem(ItemEntry itemEntry, byte[] value){
        this.itemEntry = itemEntry;
        this.value = value;
    }
}
