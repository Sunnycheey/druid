/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.client.cache;
import org.apache.druid.java.util.common.logger.Logger;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
*/
class ByteCountingLRUMap extends LinkedHashMap<ByteBuffer, byte[]>
{
  private static final Logger log = new Logger(ByteCountingLRUMap.class);

  private final boolean logEvictions;
  private final int logEvictionCount;
  private final long sizeInBytes;

  private final AtomicLong numBytes;
  private final AtomicLong evictionCount;
  private final AtomicLong lruPutNums;
  private final AtomicLong totalPutNums;
  private final AtomicLong orfEvictionNums;

  private final AtomicLong scaledLFUEvictionNums;

  private final List<ByteBuffer> keysToRemove;


  /**
  private String Byte2String(ByteBuffer buffer){
    Charset charset = StandardCharsets.UTF_8;
    CharBuffer charBuffer = charset.decode(buffer);
    String s = charBuffer.toString();
    return s;
  }
   */

  public ByteCountingLRUMap(
      final long sizeInBytes
  )
  {
    this(16, 0, sizeInBytes);
  }

  public ByteCountingLRUMap(
      final int initialSize,
      final int logEvictionCount,
      final long sizeInBytes
  )
  {
    super(initialSize, 0.75f, true);
    this.logEvictionCount = logEvictionCount;
    this.sizeInBytes = sizeInBytes;

    logEvictions = logEvictionCount != 0;
    numBytes = new AtomicLong(0L);//cache中的字节数
    evictionCount = new AtomicLong(0L);//eviction的Cache个数
    lruPutNums = new AtomicLong(0);
    totalPutNums = new AtomicLong(0);
    orfEvictionNums = new AtomicLong(0);

    scaledLFUEvictionNums = new AtomicLong(0);

    keysToRemove = new ArrayList<>();
  }

  public long getNumBytes()
  {
    return numBytes.get();
  }

  public long getEvictionCount()
  {
    return evictionCount.get();
  }

  public long getLruPutNums(){return lruPutNums.get();}

  public long getTotalPutNums(){return totalPutNums.get();}

  public long getOrfEvictionNums(){return orfEvictionNums.get();}

  public long getScaledLFUEvictionNums(){return  scaledLFUEvictionNums.get();}

  public static int byte2Int(byte[] a, int index){
    int res = 0;
    for(int i=index;i<4+index;i++){
      res += (a[i] & 0xff) << ((i-index)*8);
    }
    return res;
  }

  public static byte[] int2Byte(int a){
    byte[] tmpByte = new byte[4];
    tmpByte[0] = (byte) (a & 0xff);
    tmpByte[1] = (byte) (a >> 8 & 0xff);
    tmpByte[2] = (byte) (a >> 16 & 0xff);
    tmpByte[3] = (byte) (a >> 24 & 0xff);
    return tmpByte;
  }

  /**
   * 应该Override get方法
   * 从链表中删除该item ，然后在插入到链表的头部
   * 并返回value，如果链表中没有对应的数据，就返回NULL
   * */
  //@Override
  public byte[] get(Object key, int dataIndex){
    byte[] value = super.get(key);
    if(value != null){
      super.remove(key);
      /**
       * 这样put是否有问题 ？？ TODO
       * */
      int lfu = byte2Int(value,0);
      lfu+=1;
      byte[] tmpByte = int2Byte(lfu);
      value[0] = tmpByte[0];
      value[1] = tmpByte[1];
      value[2] = tmpByte[2];
      value[3] = tmpByte[3];

      /**
       * 更新dataIndex
       * */
      byte[] indexByte = int2Byte(dataIndex);
      value[4] = indexByte[0];
      value[5] = indexByte[1];
      value[6] = indexByte[2];
      value[7] = indexByte[3];

      super.put((ByteBuffer) key,value);
    }
    return value;
  }




  @Override
  public byte[] put(ByteBuffer key, byte[] value)
  {
    numBytes.addAndGet(key.remaining() + value.length + 4);
    Iterator<Map.Entry<ByteBuffer, byte[]>> it = entrySet().iterator();

    long totalEvictionSize = 0L;
    while (numBytes.get() - totalEvictionSize > sizeInBytes && it.hasNext()) {
      evictionCount.incrementAndGet();
      if (logEvictions && evictionCount.get() % logEvictionCount == 0) {
        log.error(
            "Evicting %,dth element.  Size[%,d], numBytes[%,d], averageSize[%,d]",
            evictionCount.get(),
            size(),
            numBytes.get(),
            numBytes.get() / size()
        );
      }

      Map.Entry<ByteBuffer, byte[]> next = it.next();
      totalEvictionSize += next.getKey().remaining() + next.getValue().length;
      keysToRemove.add(next.getKey());
    }

    for (ByteBuffer keyToRemove : keysToRemove) {
      int dataIndex = byte2Int(this.get(keyToRemove),4);
      log.error(System.currentTimeMillis()+"  ByteCountingLRUMap.put evict from cache--->key:"+keyToRemove.hashCode()
      +" dataIndex:"+dataIndex);
      remove(keyToRemove);
    }
    keysToRemove.clear();

    byte[] old = super.put(key, value);
    /**
     * old不是空 说明是之前有值，返回旧的值  对应numBytes要更新
     * */
    if (old != null) {
      numBytes.addAndGet(-key.remaining() - old.length);
    }
    return old;
  }

  @Override
  public byte[] remove(Object key)
  {
    byte[] value = super.remove(key);
    if (value != null) {
      long delta = -((ByteBuffer) key).remaining() - value.length;
      //System.out.println("before delete:"+delta+"  numBytes:"+numBytes.get());
      numBytes.addAndGet(delta);
      //System.out.println("delete:"+delta+"  numBytes:"+numBytes.get());
    }
    return value;
  }

  /**
   * Don't allow key removal using the underlying keySet iterator
   * All removal operations must use ByteCountingLRUMap.remove()
   */
  @Override
  public Set<ByteBuffer> keySet()
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
