package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.github.benmanes.caffeine.cache.Cache;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.BlogComments;
import com.hmdp.mapper.BlogCommentsMapper;
import com.hmdp.service.IBlogCommentsService;
import com.hmdp.service.IBlogService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_BLOG_COMMENTS_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_BLOG_COMMENTS_TTL;

@Service
public class BlogCommentsServiceImpl extends ServiceImpl<BlogCommentsMapper, BlogComments> implements IBlogCommentsService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private Cache<String, Object> localCache;
    @Resource
    private CacheClient cacheClient;
    @Resource
    private IBlogService blogService;

    @Override
    @SuppressWarnings("unchecked")
    public Result queryCommentsByBlogId(Long blogId, Integer current) {
        if (blogId == null) {
            return Result.fail("笔记id不能为空");
        }
        int pageNo = current == null || current < 1 ? 1 : current;
        String key = buildCacheKey(blogId, pageNo);

        Object localValue = localCache.getIfPresent(key);
        if (localValue != null) {
            return Result.ok((List<BlogComments>) localValue);
        }

        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(json)) {
            List<BlogComments> comments = JSONUtil.toList(JSONUtil.parseArray(json), BlogComments.class);
            localCache.put(key, comments);
            return Result.ok(comments);
        }

        Page<BlogComments> page = query()
                .eq("blog_id", blogId)
                .eq("status", false)
                .orderByDesc("liked")
                .orderByDesc("create_time")
                .page(new Page<>(pageNo, SystemConstants.DEFAULT_PAGE_SIZE));
        List<BlogComments> comments = page.getRecords();
        if (comments == null) {
            comments = Collections.emptyList();
        }
        cacheClient.set(key, comments, CACHE_BLOG_COMMENTS_TTL, TimeUnit.MINUTES);
        return Result.ok(comments);
    }

    @Override
    @Transactional
    public Result saveComment(BlogComments comment) {
        if (comment == null || comment.getBlogId() == null || StrUtil.isBlank(comment.getContent())) {
            return Result.fail("评价内容不能为空");
        }
        if (blogService.getById(comment.getBlogId()) == null) {
            return Result.fail("笔记不存在");
        }
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return Result.fail("请先登录");
        }

        comment.setUserId(user.getId());
        if (comment.getParentId() == null) {
            comment.setParentId(0L);
        }
        if (comment.getAnswerId() == null) {
            comment.setAnswerId(0L);
        }
        if (comment.getLiked() == null) {
            comment.setLiked(0);
        }
        if (comment.getStatus() == null) {
            comment.setStatus(false);
        }

        boolean success = save(comment);
        if (!success) {
            return Result.fail("发布评价失败");
        }
        blogService.update().setSql("comments = IFNULL(comments, 0) + 1").eq("id", comment.getBlogId()).update();
        evictBlogCommentsCache(comment.getBlogId());
        return Result.ok(comment.getId());
    }

    private String buildCacheKey(Long blogId, Integer current) {
        return CACHE_BLOG_COMMENTS_KEY + blogId + ":" + current;
    }

    private void evictBlogCommentsCache(Long blogId) {
        String prefix = CACHE_BLOG_COMMENTS_KEY + blogId + ":";
        localCache.asMap().keySet().removeIf(key -> key.startsWith(prefix));
        Set<String> keys = stringRedisTemplate.keys(prefix + "*");
        if (keys != null && !keys.isEmpty()) {
            stringRedisTemplate.delete(keys);
        }
    }
}
