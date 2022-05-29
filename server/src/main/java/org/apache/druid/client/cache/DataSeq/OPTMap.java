package org.apache.druid.client.cache.DataSeq;

import org.apache.druid.client.cache.Item.ItemEntry;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class OPTMap extends LinkedHashMap<String, ItemEntry> {
    private static final Logger log = new Logger(OPTMap.class);

    private final long sizeInBytes;
    private final AtomicLong numBytes;

    public OPTMap(int cacheSize){
        super(cacheSize,0.75f,true);
        this.sizeInBytes = cacheSize;
        numBytes = new AtomicLong(0L);
    }

    public long getNumBytes()
    {
        return numBytes.get();
    }
    public long getSizeInBytes() {return  sizeInBytes;}

    @Override
    public ItemEntry put(String key, ItemEntry value)
    {
        numBytes.addAndGet(value.getValueSize());
        ItemEntry tmp = super.put(key, value);
        if(tmp!=null){
            numBytes.addAndGet(-tmp.getValueSize());
            return tmp;
        }
        return tmp;
    }

    @Override
    public ItemEntry remove(Object key)
    {
        ItemEntry value = super.remove(key);
        if (value != null) {
            numBytes.addAndGet(-value.getValueSize());
        }
        return value;
    }

    @Override
    public Set<String> keySet()
    {
        return Collections.unmodifiableSet(super.keySet());
    }

    @Override
    public void clear()
    {
        numBytes.set(0L);
        super.clear();
    }

}
