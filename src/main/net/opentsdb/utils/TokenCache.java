package net.opentsdb.utils;

import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Created by yuanxiaolong on 2018/1/29.
 */
public class TokenCache {

    private static Logger LOG = LoggerFactory.getLogger(TokenCache.class);

    private static final String COLUMN_FAMILY = "u";

    private static final String TOKEN_QUALIFIER = "token";

    private static final Long _1_HOUR = 3600000L;

    /**
     * token -- TTL Mapping
     */
    private static Cache<String, Long> tokenTTLCache = callableCached(10000, 1, TimeUnit.HOURS);

    /**
     * token -- RowKey Mapping
     */
    private static Cache<String, String> tokenRowKey = callableCached(10000, 1, TimeUnit.HOURS);

    /**
     * token -- RowKey Mapping
     */
    private static Cache<String, String> rowKeyToken = callableCached(10000, 1, TimeUnit.HOURS);

    /**
     * 对需要延迟处理的可以采用这个机制；(泛型的方式封装)
     * @param maxNum
     * @param expire
     * @param unit
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K,V> Cache<K , V> callableCached(int maxNum, int expire, TimeUnit unit){
        Cache<K, V> cache = CacheBuilder.newBuilder()
                .maximumSize(maxNum)
                .expireAfterWrite(expire, unit)
                .removalListener(new RemovalListener<K, V>() {
                    @Override
                    public void onRemoval(RemovalNotification<K, V> kv) {
                        LOG.debug(kv.getKey() + " was removed from loading cache, cause:{}...", kv.getCause());
                    }
                })
                .build();

        return cache;
    }

    /**
     *
     * @param token
     * @param rowKey
     */
    public static void put2TokenRowKey(final String token, final String rowKey, final Long ttl){
        tokenTTLCache.put(token, ttl);
        tokenRowKey.put(token, rowKey);
        rowKeyToken.put(rowKey, token);
    }

    /**
     * 根据Token获取RowKey
     * @param token
     * @return
     */
    public static String getRowKey(final String token){
        try{
            Long ttl = tokenTTLCache.getIfPresent(token);
            if(null == ttl || ttl.longValue() < System.currentTimeMillis()){//已经过期
                tokenTTLCache.invalidate(token);
                tokenRowKey.invalidate(token);
                return null;
            }
            return tokenRowKey.getIfPresent(token);
        } catch(Exception e){
            e.printStackTrace();
            LOG.error("scan HBase acquire RowKey error:{}", e);
        }

        return null;
    }

    /**
     * 根据RowKey获取Token
     * @param rowKey
     * @return
     */
    public static String getToken(final String rowKey){
        String token = rowKeyToken.getIfPresent(rowKey);
        if(Strings.isNullOrEmpty(token)) return token;
        Long ttl = tokenTTLCache.getIfPresent(token);
        if(null != ttl && ttl.longValue() > System.currentTimeMillis()){
            return token;
        }else{
            tokenTTLCache.invalidate(token);
            rowKeyToken.invalidate(rowKey);
            return null;
        }
    }

}
