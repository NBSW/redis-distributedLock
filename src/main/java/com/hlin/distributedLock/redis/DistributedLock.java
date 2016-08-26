package com.hlin.distributedLock.redis;

/**
 * 抽象分布式锁
 * 
 * @author hailin0@yeah.net
 * @createDate 2016年8月18日
 * 
 */
public abstract class DistributedLock {

    /**
     * 获取锁
     * 
     * @return
     */
    public boolean tryLock() {
        try {
            return lock();
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 获取锁
     * 
     * @param timeout 获取所超时时间(毫秒值)
     * @return
     */
    public boolean tryLock(long timeout) {
        do {
            if (tryLock()) {
                return true;
            }
            timeout -= 100;
            try {
                Thread.sleep(Math.min(100, 100 + timeout));
            } catch (InterruptedException e) {
            }
        } while (timeout > 0);

        return false;
    }

    /**
     * 获取锁
     * 
     * @return
     */
    protected abstract boolean lock();

    /**
     * 检查所是否有效,锁是否存在or锁是否已被其他进程重新获取
     * 
     * @return
     */
    public abstract boolean check();

    /**
     * 维持心跳，仅在heartbeatTime < timeout时需要
     * <p>
     * 如果heartbeatTime == timeout，此操作没有意义，因为在心跳时key已失效了
     * 
     * @return
     */
    public abstract boolean heartbeat();

    /**
     * 释放锁
     */
    public abstract boolean unLock();

}
