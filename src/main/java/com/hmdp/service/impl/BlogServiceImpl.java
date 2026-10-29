package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.BeanUtils;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IUserService userService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IFollowService followService;


    @Override
    public Object queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            queryByBlogUser(blog);
            isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result queryBlogById(Long id) {
        Blog blog = getById(id);
        if (blog == null){
            return Result.fail("博客不存在");
        }
        queryByBlogUser(blog);
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    @Override
    public Result likeBlog(Long id) {
        //获取当前用户
        Long userId = UserHolder.getUser().getId();
        //判断当前用户是否已经点赞
        Double score = stringRedisTemplate.opsForZSet().score(RedisConstants.BLOG_LIKED_KEY + id, userId.toString());
        if (score == null) {
            //未点赞，可以点赞
            //数据库点赞+1
            boolean isSuccess  = update().setSql("liked = liked + 1")
                    .eq("id", id)
                    .update();
            //保存用户到redis的set集合
            if (isSuccess) {
                stringRedisTemplate.opsForZSet().add(RedisConstants.BLOG_LIKED_KEY + id, userId.toString(), System.currentTimeMillis());
            }

        }else {
            //已点赞，取消点赞
            //数据库点赞-1
            boolean isSuccess  = update().setSql("liked = liked - 1")
                    .eq("id", id)
                    .update();
            //把用户从redis的set集合中移除
            if (isSuccess) {
                stringRedisTemplate.opsForZSet().remove(RedisConstants.BLOG_LIKED_KEY + id, userId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        //获取top5的点赞用户
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(RedisConstants.BLOG_LIKED_KEY + id, 0, 4);
        if (top5 == null || top5.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        //解析其中的用户id
        List<Long> ids = top5.stream()
                .map(Long::valueOf)
                .collect(Collectors.toList());
        String idsStr = StrUtil.join(",", ids);
        //根据id获取用户
        List<UserDTO> userDTOS = userService.query()
                .in("id", ids)
                .last("ORDER BY FIELD(id , " + idsStr + ")")
                .list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(userDTOS);
    }

    @Override
    public Result saveBlog(Blog blog) {
        //获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        //保存探店blog
        boolean isSuccess = save(blog);
        if (!isSuccess) {
            return Result.fail("保存失败");
        }
        //查询作者所有粉丝
        List<Follow> follows = followService.query().eq("follow_user_id", user.getId()).list();
        //推送笔记id给所有粉丝
        for (Follow follow : follows) {
            //获取粉丝id
            Long userId = follow.getUserId();
            //推送
            String key = RedisConstants.FEED_KEY + userId;
            stringRedisTemplate.opsForZSet().add(key, blog.getId().toString(), System.currentTimeMillis());
        }
        //返回结果
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        //获取当前用户
        Long userId = UserHolder.getUser().getId();
        //查询收件箱
        String key = RedisConstants.FEED_KEY + userId;
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, max, offset, 2);
        //非空判断
        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }
        //解析数据：blogid，minTime（时间戳），offset
        List<Long> ids = new ArrayList<>(typedTuples.size());
        long minTime = 0;
        int os = 1;
        for (ZSetOperations.TypedTuple<String> tuple : typedTuples) {
            //获取blogId
            ids.add(Long.valueOf(tuple.getValue()));
            //获取minTime
            long time = tuple.getScore().longValue();
            //获取offset
            if (time == minTime){
                os++;
            }else {
                minTime = time;
                os = 1;
            }

        }
        //根据id查询blog
        List<Blog> blogs = query().in("id", ids).last("ORDER BY FIELD(id, " + StrUtil.join(",", ids) + ")").list();
        blogs.forEach(blog -> {
            queryByBlogUser(blog);
            isBlogLiked(blog);
        });

        //封装并返回
        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(blogs);
        scrollResult.setMinTime(minTime);
        scrollResult.setOffset(os);
        return Result.ok(scrollResult);

    }


    private void isBlogLiked(Blog blog) {
        //获取当前用户
        UserDTO user = UserHolder.getUser();
        if (user == null){
            return;
        }
        Long userId = user.getId();
        //判断当前用户是否已经点赞
        Double score = stringRedisTemplate.opsForZSet().score(RedisConstants.BLOG_LIKED_KEY + blog.getId(), userId.toString());
        blog.setIsLike(score != null);
    }

    private void queryByBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}
