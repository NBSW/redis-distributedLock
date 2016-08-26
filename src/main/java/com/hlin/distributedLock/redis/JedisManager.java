package com.hlin.distributedLock.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

/**
 * 分片的redis客户端
 * 
 * @author hailin0@yeah.net
 * @createDate 2016年3月3日
 * 
 */
public class JedisManager {

    private static final Logger log = LoggerFactory.getLogger(JedisManager.class);

    private ShardedJedisPool shardedJedisPool;

    /**
     * @param shardedJedisPool
     */
    public JedisManager(ShardedJedisPool shardedJedisPool) {
        this.shardedJedisPool = shardedJedisPool;
    }

    /**
     * setnx
     * 
     * @param key
     * @param value
     * @param seconds
     * @return
     */
    public boolean setnx(String key, String value, int seconds) {
        ShardedJedis shardedJedis = shardedJedisPool.getResource();
        try {
            Long setnx = shardedJedis.setnx(key.getBytes(), value.getBytes());
            if (setnx == 1 && seconds != -1) {
                shardedJedis.expire(key, seconds);
                return true;
            }
            return false;
        } catch (Exception e) {
            shardedJedis.close();
            return false;
        }
    }

    /**
     * 设置超时时间
     * 
     * @param key
     * @param seconds
     * @return
     */
    public boolean expire(String key, int seconds) {
        ShardedJedis shardedJedis = shardedJedisPool.getResource();
        try {
            if (shardedJedis.exists(key)) {

            }
            return shardedJedis.expire(key, seconds) == 1 ? true : false;
        } catch (Exception e) {
            shardedJedis.close();
            return false;
        }
    }

    /**
     * get
     * 
     * @param key
     * @return
     */
    public String get(String key) {
        ShardedJedis shardedJedis = shardedJedisPool.getResource();
        try {
            return shardedJedis.get(key);
        } catch (Exception e) {
            shardedJedis.close();
            return null;
        }
    }

    /**
     * getSet
     * 
     * @param key
     * @return
     */
    public String getSet(String key, String value) {
        ShardedJedis shardedJedis = shardedJedisPool.getResource();
        try {
            return shardedJedis.getSet(key, value);
        } catch (Exception e) {
            shardedJedis.close();
            return null;
        }
    }

    /**
     * del
     * 
     * @param key
     */
    public boolean del(String key) {
        ShardedJedis shardedJedis = shardedJedisPool.getResource();
        try {
            return shardedJedis.del(key) == 1 ? true : false;
        } catch (Exception e) {
            shardedJedis.close();
            return false;
        }
    }

}