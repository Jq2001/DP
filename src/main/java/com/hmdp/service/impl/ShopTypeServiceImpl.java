package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOPTYPE_LIST_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOPTYPE_LIST_TTL;

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
    public List<ShopType> queryShopType() {
        String shopTypeJson = stringRedisTemplate.opsForValue().get(CACHE_SHOPTYPE_LIST_KEY);
        if (StrUtil.isNotBlank(shopTypeJson)) {
            return JSONUtil.toList(JSONUtil.parseArray(shopTypeJson), ShopType.class);
        }
        List<ShopType> typeList = query().orderByAsc("sort").list();

        if (typeList == null) {
            typeList = Collections.emptyList();
        }
        stringRedisTemplate.opsForValue().set(
                CACHE_SHOPTYPE_LIST_KEY,
                JSONUtil.toJsonStr(typeList),
                CACHE_SHOPTYPE_LIST_TTL,
                TimeUnit.MINUTES
                );
        return typeList;
    }
}
