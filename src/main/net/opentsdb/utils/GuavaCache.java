package net.opentsdb.utils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Created by yuanxiaolong on 2018/1/23.
 */
public class GuavaCache {

    final static Cache<String, String> cache = CacheBuilder.newBuilder()
            //设置cache的初始大小为10，要合理设置该值
            .initialCapacity(10)
            //默认值为-1，无限制?
//            .maximumSize(10000)
            //设置并发数为5，即同一时间最多只能有5个线程往cache执行写入操作
            .concurrencyLevel(5)
            //设置cache中的数据在写入之后的存活时间为10秒
            .expireAfterWrite(30, TimeUnit.MINUTES)
            //构建cache实例
            .build();

    /**
     *
     * @param token
     * @param rowKey
     */
    public static void cache(String token, String rowKey){
        cache.put(token, rowKey);
    }

    /**
     *
     * @param token
     * @return
     */
    public static String getRowKey(String token){
        return cache.getIfPresent(token);
    }
}
