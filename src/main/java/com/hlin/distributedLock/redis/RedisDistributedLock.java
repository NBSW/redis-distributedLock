package com.hlin.distributedLock.redis;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

/**
 * 
 * redis分布式锁
 * 
 * @author hailin1.wang@downjoy.com
 * @createDate 2016年8月18日
 * 
 */
public class RedisDistributedLock extends DistributedLock {

    /**
     * redis操作数据源
     */
    private JedisManager jedisManager = JedisManagerFactory.getJedisManager();

    /**
     * lock的key
     */
    private String key;

    /**
     * lock的心跳时间(毫秒)
     * <p>
     * 必须满足(heartbeatTime <= timeout*1000)
     */
    private long heartbeatTime;

    /**
     * lock的自然超时时间(秒)
     */
    private int timeout;

    /**
     * 版本号时间，作为获取锁的客户端操作的依据
     */
    private long versionTime;

    /**
     * 是否快速失败，只进行一次最简单的setnx竞争
     */
    private boolean fastfail;

    /**
     * 
     * @param key
     * @param timeout
     */
    public RedisDistributedLock(String key, int timeout) {
        this(key, timeout * 1000, timeout, true);
    }

    /**
     * 
     * @param key
     * @param heartbeatTime 必须满足(heartbeatTime <= timeout*1000)
     * @param timeout
     */
    public RedisDistributedLock(String key, long heartbeatTime, int timeout) {
        this(key, heartbeatTime, timeout, (heartbeatTime == timeout * 1000 ? true : false));
    }

    /**
     * 
     * @param key
     * @param heartbeatTime 必须满足(heartbeatTime <= timeout*1000)
     * @param timeout
     * @param fastfail 快速失败只在heartbeatTime == timeout * 1000时才有意义
     */
    public RedisDistributedLock(String key, long heartbeatTime, int timeout, boolean fastfail) {
        Preconditions.checkArgument(heartbeatTime <= timeout * 1000,
                "info:heartbeatTime 必须满足(heartbeatTime <= timeout*1000) ");
        this.key = key;
        this.heartbeatTime = heartbeatTime;
        this.timeout = timeout;
        this.fastfail = fastfail;
    }

    /**
     * 获取锁
     * <p>
     * 1.通过timeout控制持有锁对象失去响应导致的最终超时
     * <p>
     * 2.通过heartbeatTime中存放的时间戳内容控制锁的释放，只能释放自己的锁,同时也可以作为锁的心跳检测
     * 
     * @return
     */
    protected boolean lock() {
        // 1. 通过setnx试图获取一个lock,setnx成功，则成功获取一个锁
        boolean setnx = jedisManager.setnx(key, buildVal(), timeout);
        // 快速失败，锁获取成功返回true,锁获取失败并且快速失败的为true则直接返回false
        if (setnx || fastfail) {
            return setnx ? true : false;
        }

        // 2. setnx失败，说明锁仍然被其他对象保持，检查其是否已经超时并获取，未超时，则直接返回失败
        long oldValue = getLong(jedisManager.get(key));
        //key在当前方法执行过程中失效,再进行一次竞争
        if(oldValue == 0){
            return jedisManager.setnx(key, buildVal(), timeout);
        }
        //未超时
        if (oldValue > System.currentTimeMillis()) {
            return false;
        }

        // 3.已经超时,则获取锁
        long getSetValue = getLong(jedisManager.getSet(key, buildVal()));
        // key在当前方法执行过程中失效，再进行一次竞争
        if (getSetValue == 0) {
            return jedisManager.setnx(key, buildVal(), timeout);
        }
        // 已被其他进程获取(已被其他进程通过getset设置新值)
        if (getSetValue != oldValue) {
            return false;
        }

        // 4.续租过期时间
        if (jedisManager.expire(key, timeout)) {
            return true;
        }
        //续租失败了,key可能失效了，则再获取一次
        String time = getVal();
        if(jedisManager.setnx(key, time, timeout)){
            versionTime = Long.valueOf(time);
            return true;
        }

        //5.无法续租，也无法获取
        //续租失败（续租过程中key失效然后key又被其他进程获取），比对val值
        if(versionTime != getLong(jedisManager.get(key))){
            //已经被获取
            return false;
        }
        //续租失败（可能为网络原因引起的），但key未失效，再续租一次
        if (jedisManager.expire(key, timeout)) {
            return true;
        }

        //6.最后还是无法续租锁（已经确定是自己的锁），则可以释放锁从新获取一次
        if(unLock()){
            return jedisManager.setnx(key, buildVal(), timeout);
        }
        Thread.currentThread().
        throw new RuntimeException("获取锁异常，原因为：获取锁成功，但无法对锁进行续租！并且无法释放锁！");
    }

    /**
     * 检查所是否有效,锁是否超时or锁是否已被其他进程重新获取
     * 
     * @return
     */
    public boolean check() {
        long getVal = getLong(jedisManager.get(key));
        return System.currentTimeMillis() < getVal && versionTime == getVal;
    }

    /**
     * 维持心跳，仅在heartbeatTime < timeout时需要
     * <p>
     * 如果heartbeatTime == timeout，此操作是没有意义的
     * 
     * @return
     */
    @Override
    public boolean heartbeat() {
        // 1. 避免操作非自己获取得到的锁
        return check() && getLong(jedisManager.getSet(key, buildVal())) != 0;
    }

    /**
     * 释放锁
     */
    public boolean unLock() {
        // 1. 避免删除非自己获取得到的锁
        return check() && jedisManager.del(key);
    }

    /**
     * if value==null || value=="" <br>
     * &nbsp return 0 <br>
     * else <br>
     * &nbsp return Long.valueOf(value)
     * 
     * @param value
     * @return
     */
    private long getLong(String value) {
        return StringUtils.isBlank(value) ? 0 : Long.valueOf(value);
    }

    /**
     * 生成val,当前系统时间+心跳时间
     * 
     * @return System.currentTimeMillis() + heartbeatTime + 1
     */
    private String buildVal() {
        versionTime = System.currentTimeMillis() + heartbeatTime + 1;
        return String.valueOf(versionTime);
    }

    /**
     * 生成val,当前系统时间+心跳时间
     *
     * @return System.currentTimeMillis() + heartbeatTime + 1
     */
    private String getVal() {
        long time = System.currentTimeMillis() + heartbeatTime + 1;
        return String.valueOf(time);
    }
}
