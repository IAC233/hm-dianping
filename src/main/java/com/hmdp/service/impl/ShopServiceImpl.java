package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private static final Random RANDOM = new Random();

    /**
     * 根据id查询商铺信息
     * @param id 商铺id
     * @return 商铺信息
     */
    @Override
    public Result queryById(Long id) {
        //缓存穿透
        //Shop shop = queryWithPassThrough(id);
        //缓存击穿(互斥锁)+缓存穿透(写入null值)+缓存雪崩(随机TTL)
        //Shop shop = queryWithMutex(id);
        //缓存击穿(逻辑过期)
        Shop shop = queryWithLogicalExpire(id);
        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        //返回
        return Result.ok(shop);
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    public Shop queryWithLogicalExpire(Long id) {
        //从redis查缓存
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        //判断是否存在
        if (StrUtil.isBlank(shopJson)) {
            //不存在，返回
            return null;
        }
        //命中，将json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        //判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            //未过期，返回店铺信息
            return shop;
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
                    this.saveShop2Redis(id, 20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    //释放锁
                    unlock(lockKey);
                }
            });
        }
        //未成功，返回商铺信息
        return shop;

    }

    /**
     * 根据id查询商铺信息
     * 缓存击穿方法封装
     * @param id 商铺id
     * @return 商铺信息
     */
    public Shop queryWithMutex  (Long id) {
        //从redis查缓存
            String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
            //判断是否存在
            if (StrUtil.isNotBlank(shopJson)) {
                //存在，返回
                return JSONUtil.toBean(shopJson, Shop.class);
            }
            if (shopJson != null) {
                //返回错误信息
                return null;
            }
            //实现缓存重建
            //获取互斥锁
        Shop shop = null;
        try {
            boolean isLock = tryLock(RedisConstants.LOCK_SHOP_KEY + id);
            //判断是否获取锁
            if (!isLock) {
                //否，休眠一段时间并重试
                Thread.sleep(100);
                return queryWithMutex(id);
            }
            //是，查询数据库
            shop = getById(id);
            //数据库不存在，返回错误
            if (shop == null) {
                //将空值写入redis
                long nullTtl = RedisConstants.CACHE_NULL_TTL;
                long randomNull = RANDOM.nextInt(5);
                stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, "", nullTtl + randomNull, TimeUnit.MINUTES);
                return null;
            }
            //存在，写入redis
            //缓存雪崩
            long cacheTtl = RedisConstants.CACHE_SHOP_TTL;
            long randomCacheTtl = RANDOM.nextInt(5);
            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), cacheTtl + randomCacheTtl, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            //释放互斥锁
            unlock(RedisConstants.LOCK_SHOP_KEY + id);
        }
            //返回
            return shop;

    }

    /**
     * 根据id查询商铺信息
     * 缓存穿透方法封装
     * @param id 商铺id
     * @return 商铺信息
     */
    public Shop queryWithPassThrough(Long id) {
        //从redis查缓存
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        //判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //存在，返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //判断是否为空值
        if (shopJson != null) {
            //返回错误信息
            return null;
        }
        //不存在，查询数据库
        Shop shop = getById(id);
        //数据库不存在，返回错误
        if (shop == null) {
            //将空值写入redis
            long nullTtl = RedisConstants.CACHE_NULL_TTL;
            long randomNull = RANDOM.nextInt(5);
            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, "", nullTtl + randomNull, TimeUnit.MINUTES);
            return null;
        }
        //存在，写入redis
        long cacheTtl = RedisConstants.CACHE_SHOP_TTL;
        long randomCacheTtl = RANDOM.nextInt(5);
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), cacheTtl + randomCacheTtl, TimeUnit.MINUTES);
        //返回
        return shop;
    }

    /**
     * 尝试获取锁
     * @param key 锁的key
     * @return 是否获取成功
     */
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", RedisConstants.LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 释放锁
     * @param key 锁的key
     */
    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

    /**
     * 将商铺信息保存到redis中，并设置逻辑过期时间
     * @param id 商铺id
     * @param expireSeconds 过期时间
     */
    private void saveShop2Redis(Long id, long expireSeconds) {
        //查询店铺数据
        Shop shop = getById(id);
        //封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //写入redis
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }
    /**
     * 更新商铺信息
     * @param shop 商铺信息
     * @return 更新结果
     */
    @Transactional
    @Override
    public Result updateShop(Shop shop) {
        Long id = shop.getId();
        if (id == null){
            return Result.fail("店铺id不能为空");
        }
        updateById(shop);
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + shop.getId());
        return Result.ok();
    }
}
