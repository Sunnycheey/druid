package org.apache.druid.client.cache;

public class ORFCacheProvider extends ORFCacheConfig implements CacheProvider{
    @Override
    public Cache get() {
        return ORFCache.create(this);
    }
}
