package com.hmdp.service.impl;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result queryTypeList() {
        List<String> shopTypeListJson = stringRedisTemplate.opsForList().range(RedisConstants.CACHE_SHOP_TYPE_KEY, 0, -1);
        if (CollectionUtil.isNotEmpty(shopTypeListJson)){
            List<ShopType> shopTypeList = shopTypeListJson.stream()
                    .map(str-> JSONUtil.toBean(str, ShopType.class))
                    .sorted(Comparator.comparingInt(ShopType::getSort))
                    .collect(Collectors.toList());
            return Result.ok(shopTypeList);
        }
        List<ShopType> shopTypeList = query().orderByAsc("sort").list();
        if (CollectionUtil.isEmpty(shopTypeList)){
            return Result.fail("店铺类型查询失败");
        }
        shopTypeListJson = shopTypeList.stream()
                .map(shopType -> JSONUtil.toJsonStr(shopType))
                .collect(Collectors.toList());
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_TYPE_KEY);
        stringRedisTemplate.opsForList().rightPushAll(RedisConstants.CACHE_SHOP_TYPE_KEY, shopTypeListJson);
        return Result.ok(shopTypeList);
    }
}
