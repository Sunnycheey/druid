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

/**
 */
public class CacheStats
{
  private final long numHits;
  private final long numMisses;
  private final long size;
  private final long sizeInBytes;
  private final long numEvictions;
  private final long numTimeouts;
  private final long numErrors;

  private long lruNums;
  private long totalNums;
  private long orfEvictionNums;

  public CacheStats(
      long numHits,
      long numMisses,
      long size,
      long sizeInBytes,
      long numEvictions,
      long numTimeouts,
      long numErrors
  )
  {
    this.numHits = numHits;
    this.numMisses = numMisses;
    this.size = size;
    this.sizeInBytes = sizeInBytes;
    this.numEvictions = numEvictions;
    this.numTimeouts = numTimeouts;
    this.numErrors = numErrors;

    this.orfEvictionNums = 0;
    this.lruNums = 0;
    this.totalNums = 0;
  }


  public CacheStats(
          long numHits,
          long numMisses,
          long size,
          long sizeInBytes,
          long numEvictions,
          long lruNums,
          long totalNums,
          long orfEvictionNums,
          long numTimeouts,
          long numErrors
  )
  {
    this.numHits = numHits;
    this.numMisses = numMisses;
    this.size = size;
    this.sizeInBytes = sizeInBytes;
    this.numEvictions = numEvictions;
    this.lruNums = lruNums;
    this.totalNums = totalNums;
    this.orfEvictionNums = orfEvictionNums;
    this.numTimeouts = numTimeouts;
    this.numErrors = numErrors;
  }

  @Override
  public String toString() {
    return "CacheStats{" +
            "numHits=" + numHits +
            "\n numMisses=" + numMisses +
            "\n size=" + size +
            "\n sizeInBytes=" + sizeInBytes +
            "\n numEvictions=" + numEvictions +
            "\n numTimeouts=" + numTimeouts +
            "\n numErrors=" + numErrors +
            "\n lruNums=" + lruNums +
            "\n totalNums=" + totalNums +
            "\n orfEvictionNums="+orfEvictionNums+
            '}';
  }

  public long getLruNums(){ return lruNums;}

  public long getTotalNums() {return totalNums;}

  public long getOrfEvictionNums(){return orfEvictionNums;}

  public long getNumHits()
  {
    return numHits;
  }

  public long getNumMisses()
  {
    return numMisses;
  }

  public long getNumEntries()
  {
    return size;
  }

  public long getSizeInBytes()
  {
    return sizeInBytes;
  }

  public long getNumEvictions()
  {
    return numEvictions;
  }

  public long getNumTimeouts()
  {
    return numTimeouts;
  }

  public long getNumErrors()
  {
    return numErrors;
  }

  public long numLookups()
  {
    return numHits + numMisses;
  }

  public double hitRate()
  {
    long lookups = numLookups();
    return lookups == 0 ? 0 : numHits / (double) lookups;
  }

  public long averageBytes()
  {
    return size == 0 ? 0 : sizeInBytes / size;
  }

  public CacheStats delta(CacheStats oldStats)
  {
    if (oldStats == null) {
      return this;
    }
    return new CacheStats(
        numHits - oldStats.numHits,
        numMisses - oldStats.numMisses,
        size - oldStats.size,
        sizeInBytes - oldStats.sizeInBytes,
        numEvictions - oldStats.numEvictions,
        numTimeouts - oldStats.numTimeouts,
        numErrors - oldStats.numErrors
    );
  }
}
