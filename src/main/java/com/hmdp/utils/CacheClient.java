package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.github.benmanes.caffeine.cache.Cache;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private Cache<String, Object> localCache;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type,
                                          Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        Object localValue = localCache.getIfPresent(key);
        if (localValue != null) {
            return type.cast(localValue);
        }

        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(json)) {
            R r = JSONUtil.toBean(json, type);
            localCache.put(key, r);
            return r;
        }
        if (json != null) {
            return null;
        }

        R r = dbFallback.apply(id);
        if (r == null) {
            stringRedisTemplate.opsForValue().set(key, "", randomizeTtl(CACHE_NULL_TTL), TimeUnit.MINUTES);
            return null;
        }
        this.set(key, r, time, unit);
        return r;
    }

    public void set(String key, Object value, Long time, TimeUnit unit) {
        if (value == null) {
            return;
        }
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), randomizeTtl(time), unit);
        localCache.put(key, value);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        if (value == null) {
            return;
        }
        RedisData redisData = new RedisData();
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(randomizeTtl(time))));
        redisData.setData(value);
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
        localCache.put(key, value);
    }

    public <R, ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type,
                                            Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        Object localValue = localCache.getIfPresent(key);
        if (localValue != null) {
            return type.cast(localValue);
        }

        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isBlank(json)) {
            return null;
        }

        RedisData redisdata = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisdata.getData(), type);
        LocalDateTime expireTime = redisdata.getExpireTime();
        if (expireTime.isAfter(LocalDateTime.now())) {
            localCache.put(key, r);
            return r;
        }

        String localKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(localKey);
        if (isLock) {
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    R r1 = dbFallback.apply(id);
                    this.setWithLogicalExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unlock(localKey);
                }
            });
        }
        return r;
    }

    public void evict(String key) {
        localCache.invalidate(key);
        stringRedisTemplate.delete(key);
    }

    private Long randomizeTtl(Long time) {
        if (time == null || time <= 0) {
            return time;
        }
        long jitter = Math.max(1L, time / 5L);
        return time + ThreadLocalRandom.current().nextLong(jitter + 1);
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }
}
