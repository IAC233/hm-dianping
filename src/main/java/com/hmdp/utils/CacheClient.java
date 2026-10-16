package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Component
@Slf4j
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }
    /**
     * 将对象写入Redis
     *
     * @param key   键
     * @param value 值
     * @param time  时间
     * @param unit  单位
     */
    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    /**
     * 将对象写入Redis，并设置逻辑过期时间
     *
     * @param key   键
     * @param value 值
     * @param time  时间
     * @param unit  单位
     */
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 通过缓存穿透查询
     *
     * @param keyPrefix 键前缀
     * @param id        缓存键值
     * @param type      缓存数据类型
     * @param dbFallback    数据库查询方法
     * @param time      缓存时间
     * @param unit      时间单位
     * @return 缓存数据
     */
    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        //从redis查缓存
        String json = stringRedisTemplate.opsForValue().get(keyPrefix + id);
        //判断是否存在
        if (StrUtil.isNotBlank(json)) {
            //存在，返回
            return JSONUtil.toBean(json, type);
        }
        //判断是否为空值
        if (json != null) {
            //返回错误信息
            return null;
        }
        //不存在，查询数据库
        R r = dbFallback.apply(id);
        //数据库不存在，返回错误
        if (r == null) {
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(keyPrefix + id, "", time, unit);
            return null;
        }
        //存在，写入redis
        this.set(keyPrefix + id, r, time, unit);
        //返回
        return r;
    }


    /**
     * 通过互斥锁查询
     *
     * @param keyPrefix 键前缀
     * @param id        缓存键值
     * @param type      缓存数据类型
     * @param dbFallback    数据库查询方法
     * @param time      缓存时间
     * @param unit      时间单位
     * @return 缓存数据
     */
    private <R,ID> R queryWithMutex(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        //1.从redis当中查询商铺缓存
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if(StrUtil.isNotBlank(json)){
            //3.存在, 返回信息
            return JSONUtil.toBean(json, type);
        }
        //判断命中的是否是空字符串, 如果是则返回空信息
        if(json != null){
            return null;
        }
        R r = null;
        //4.1 获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        try {
            //4.2 判断是否获取成功
            boolean isLock = tryLock(lockKey);
            if(!isLock){
                //4.3 失败, 休眠并重试
                Thread.sleep(50);
                return queryWithMutex(keyPrefix, id, type, dbFallback, time, unit);
            }

            // 第二次检测：再次检查缓存是否存在（防止多个线程同时重建缓存）
            json = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(json)) {
                return JSONUtil.toBean(json, type);
            }
            //4.4 成功,缓存重建  根据id查询数据库
            R r1 = dbFallback.apply(id);
            //5. 为了解决缓存穿透, 将空值写入redis, 并返回错误信息
            if(r1 == null) {
                this.set(key, "", time, unit);
                //返回错误信息
                return null;
            }
            //6. 存在 将shop对象写入redis
            this.set(key, r1, time, unit);

        } catch (InterruptedException e){
            throw new RuntimeException(e);
        } finally {
            //7.释放互斥锁
            unlock(lockKey);
        }
        //8. 返回信息
        return r;
    }
    /**
     * 通过逻辑过期查询
     *
     * @param keyPrefix 键前缀
     * @param id        缓存键值
     * @param type      缓存数据类型
     * @param dbFallback    数据库查询方法
     * @param time      缓存时间
     * @param unit      时间单位
     * @return 缓存数据
     */
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    public <R, ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        //从redis查缓存
        String json = stringRedisTemplate.opsForValue().get(keyPrefix + id);
        //判断是否存在
        if (StrUtil.isBlank(json)) {
            //不存在，返回
            return null;
        }
        //命中，将json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            //未过期，返回店铺信息
            return r;
        }
        //过期，缓存重建
        //获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        if (isLock) {
            //成功，开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    //重建缓存
                    R r1 = dbFallback.apply(id);
                    this.setWithLogicalExpire(keyPrefix + id, r1, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    //释放锁
                    unlock(lockKey);
                }
            });
        }
        //未成功，返回商铺信息
        return r;
    }

    /**
     * 尝试获取锁
     *
     * @param key 键
     * @return 是否获取成功
     */
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", RedisConstants.LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 释放锁
     *
     * @param key 键
     */
    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }
}
