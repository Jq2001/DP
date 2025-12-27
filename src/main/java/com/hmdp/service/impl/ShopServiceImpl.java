package com.hmdp.service.impl;

import cn.hutool.core.codec.PunyCode;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    @Resource
    private CacheClient cacheClient;
    @Override
    public Result queryById(Long id) {
        //用封装的工具类解决缓存穿透
//         Shop shop = cacheClient
//                 .queryWithPassThrough(CACHE_SHOP_KEY,id,Shop.class,this::getById, CACHE_SHOP_TTL,TimeUnit.MINUTES);
        //互斥锁解决缓存击穿
//        Shop shop = queryWithMutex(id);
        //逻辑过期时间解决缓存击穿
        Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY,id,Shop.class,this::getById,CACHE_SHOP_TTL,TimeUnit.MINUTES);
        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

//    private boolean tryLock(String key) {
//        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
//        return BooleanUtil.isTrue(flag);
//    }
//
//    private void unlock(String key) {
//        stringRedisTemplate.delete(key);
//    }
//
//    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
//        //查询店铺数据
//        Shop shop = getById(id);
//        Thread.sleep(200);
//        //封装逻辑过期时间
//        RedisData redisData = new RedisData();
//        redisData.setData(shop);
//        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
//        //写入redis
//        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
//    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        //1.更新数据库
        updateById(shop);
        //2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }

    /**
     * 互斥锁
     *
     * @param id
     * @return
     */
//    public Shop queryWithMutex(Long id) {
//        String key = CACHE_SHOP_KEY + id;
//        //从redis查询缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        //判断是否存在
//        if (StrUtil.isNotBlank(shopJson)) {
//            //存在 直接返回
//            return JSONUtil.toBean(shopJson, Shop.class);
//        }
//        //判断命中的是否是空值
//        if (shopJson != null) {
//            return null;
//        }
//        //不存在 实现缓存重建
//        //获取互斥锁
//        String lockKey = LOCK_SHOP_KEY + id;
//        Shop shop = null;
//        try {
//            boolean isLock = tryLock(lockKey);
//            //判断是否获取成功
//            //失败 休眠并重试
//            if (!isLock) {
//                Thread.sleep(50);
//                return queryWithMutex(id);
//            }
//            // 成功根据id 查询数据库
//            shop = getById(id);
//            //模拟重建的延时
//            Thread.sleep(200);
//            //不存在 返回错误
//            if (shop == null) {
//                //将空值写入redis
//                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
//                return null;
//            }
//            //存在 写入redis
//            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        } finally {      //释放互斥锁
//            unlock(lockKey);
//        }
//
//        //返回
//        return shop;
//    }

    /**
     * 缓存穿透
     *
     * @param id
     * @return
     */
//    public Shop queryWithPassThrough(Long id) {
//        String key = CACHE_SHOP_KEY + id;
//        //从redis查询缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        //判断是否存在
//        if (StrUtil.isNotBlank(shopJson)) {
//            //存在 直接返回
//            return JSONUtil.toBean(shopJson, Shop.class);
//        }
//        //判断命中的是否是空值
//        if (shopJson != null) {
//            return null;
//        }
//        //不存在 根据id 查询数据库
//        Shop shop = getById(id);
//        //不存在 返回错误
//        if (shop == null) {
//            //将空值写入redis
//            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
//            return null;
//        }
//        //存在 写入redis
//        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
//        //返回
//        return shop;
//    }

    /**
     * 逻辑过期
     * @param id
     * @return
     */
//    public Shop queryWithLogicalExpire(Long id) {
//        String key = CACHE_SHOP_KEY + id;
//        //从redis查询缓存
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        //判断是否存在
//        if (StrUtil.isBlank(shopJson)) {
//            //不存在 直接返回
//            return null;
//        }
//        //命中,需要先把json反序列化为对象
//        RedisData redisdata = JSONUtil.toBean(shopJson, RedisData.class);
//        Shop shop = JSONUtil.toBean((JSONObject) redisdata.getData(), Shop.class);
//        LocalDateTime expireTime = redisdata.getExpireTime();
//        //判断是否过期
//        if (expireTime.isAfter(LocalDateTime.now())){
//            //未过期 直接返回店铺信息
//            return shop;
//        }
//        //已过期 缓存重建
//        //获取互斥锁
//        String localKey = LOCK_SHOP_KEY + id;
//        boolean isLock = tryLock(localKey);
//        //判断是否获取成功
//        if (isLock) {
//            //成功 开启新线程 实现缓存重建
//            CACHE_REBUILD_EXECUTOR.submit(()->{
//                try {
//                    //重建缓存
//                    this.saveShop2Redis(id,20L);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                } finally {
//                    //释放锁
//                    unlock(localKey);
//                }
//            });
//        }
//        //返回商铺信息
//        return shop;
//    }
}
