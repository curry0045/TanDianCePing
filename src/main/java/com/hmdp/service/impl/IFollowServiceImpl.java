package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserInfoService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class IFollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Resource
    StringRedisTemplate stringRedisTemplate;

    @Resource
    private IUserService userService;

    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        //获取登录用户
        Long userId = UserHolder.getUser().getId();
        String key = "follows:" + userId;

        //1.判断是关注还是取关
        if(isFollow){
            //2.关注 新增数据
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(followUserId);

            //判断是否存入成功
            boolean isSUccess = save(follow);

            if (isSUccess){
                //把关注用户的id，放入redis的set集合 sadd userId followUserId
                stringRedisTemplate.opsForSet().add(key,followUserId.toString());
            }
        }else {
            //3.取关 删除 delete from tb_follow where user_id = ? and follow_user_id = ?
            boolean isSuccess = remove(new QueryWrapper<Follow>().eq("user_id", userId).eq("follow_user_id", followUserId));

            //取关成功 把redis中的关注记录删除
            if (isSuccess){
                //把关注用户的id从Redis集合中移除
                stringRedisTemplate.opsForSet().remove(key,followUserId.toString());
            }
        }
        return  Result.ok();
    }

    @Override
    public Result isFollow(Long followUserId) {
        //获取登录用户
        Long userId = UserHolder.getUser().getId();

        //1.查询是否关注 select count(*) from tb_follow where user_id = ? and follow_user_id = ?
        Integer count = query().eq("user_id", userId).eq("follow_user_id", followUserId).count();

        //判断
        return Result.ok(count > 0 );
    }

    @Override
    public Result followCommons(Long id) {
        //1.获取当前用户
        Long userId = UserHolder.getUser().getId();
        String key = "follows:" + userId;

        //2.求交集
        String key2 = "follows:" + id;
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key, key2);
        if (intersect == null || intersect.isEmpty()){
            //无交集 返回空的list
            return Result.ok(Collections.emptyList());
        }
        //3.解析id集合
            //Long::valueOf 等价于一个 lambda 表达式 x -> Long.valueOf(x)
            //Long 类的 valueOf 是一个静态方法，主要用于将其他类型（通常是 String 或 long）转换为 Long 包装类对象
        List<Long> ids = intersect.stream().map(Long::valueOf).collect(Collectors.toList());

        //4.查询用户
        List<UserDTO> users = userService.listByIds(ids)
                .stream()
                .map(user ->BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(users);
    }
}
