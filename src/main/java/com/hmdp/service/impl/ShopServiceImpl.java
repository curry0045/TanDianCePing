package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.fasterxml.jackson.databind.util.JSONPObject;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
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
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    StringRedisTemplate stringRedisTemplate;


    @Override
    public Result queryById(Long id) {
        //解决缓存穿透
        //Shop shop = queryWithPassThrough(id);

        //互斥锁解决缓存击穿
        //Shop shop = queryWithMutex(id);

        //逻辑过期解决缓存击穿
        Shop shop = queryWithLogicalExpire(id);

        if (shop == null){
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    //用于缓存重建的线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public Shop queryWithLogicalExpire(Long id) {
        String key = CACHE_SHOP_KEY + id;
        //1.从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StrUtil.isBlank(shopJson)) {
            //3.为空，直接返回
            return null;
        }
        //4.命中 需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        //5.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())){
            //5.1 未过期，直接返回店铺信息
            return shop;
        }


        //5.2 已过期，需要缓存重建

        //6 重建缓存
        //6.1 获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        //6.2 判断是否获取锁成功
        if (isLock){
            //6.3 成功，开启线程独立（再开启一个线程），实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    //创建缓存
                    this.saveShop2Redis(id,20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    //释放锁
                    unlock(lockKey);
                }
            });
        }

        //6.4 返回过期的店铺信息

        return shop;
    }

    //解决缓存穿透问题
    public Shop queryWithPassThrough(Long id) {
        String key = CACHE_SHOP_KEY + id;
        //1.从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //3.存在，直接返回
            //JSONUtil.toBean --> Json转对象
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        // 判断命中的是否是空值  （击穿问题）
        if (shopJson != null) {
            //返回错误信息
            return null;
        }

        //4. 不存在 根据id查询数据库
        Shop shop = getById(id);


        //4. case1 数据库中不存在，返回错误
        if (shop == null) {
            //将空值写入redis （击穿问题）
            //这里设置为“”之后 再去看 shopJson != null 的判断 能直接返回错误信息 从而不需要再去查数据库
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            //返回错误信息
            return null;

        }

            //4. case2 数据库中存在，写入redis
            // JSONUtil.toJsonStr 对象 --> Json
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);

            //5.返回
            return shop;
    }

    
    public Shop queryWithMutex(Long id) {
        String key = CACHE_SHOP_KEY + id;
        //1.从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //3.存在，直接返回
            //JSONUtil.toBean --> Json转对象
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        // 判断命中的是否是空值  （击穿问题）
        if (shopJson != null) {
            //返回错误信息
            return null;
        }

        //*实现缓存重建
        String lockKey = "locak:shop:" + id;
        Shop shop = null;
        try {
            //  *1.获取互斥锁
            boolean isLock = tryLock(lockKey);
            //  *2.判断是否获取成功
            if (!isLock){
                //*3.如果失败，则休眠并且重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }

            //  *4.如果成功，写入redis
            //4. 不存在 根据id查询数据库
            shop = getById(id);

            //模拟重建的延迟
            Thread.sleep(200);


            //4. case1 数据库中不存在，返回错误
            if (shop == null) {
                //将空值写入redis （击穿问题）
                //这里设置为“”之后 再去看 shopJson != null 的判断 能直接返回错误信息 从而不需要再去查数据库
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                //返回错误信息
                return null;

            }

            //4. case2 数据库中存在，写入redis
            // JSONUtil.toJsonStr 对象 --> Json
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        }catch (InterruptedException e){
            throw new RuntimeException(e);
        } finally {
            //  *5.释放互斥锁
            unlock(lockKey);
        }

        //5.返回
        return shop;
    }


    private boolean tryLock(String key){
        //类似redis中的setnx lock 1
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    //解决缓存击穿的第二种方法 逻辑过期时间
    public void saveShop2Redis(Long id,Long expireSeconds) throws InterruptedException {
        //1.查询店铺数据
        Shop shop = getById(id);
        Thread.sleep(200);
        //2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //3.写入Redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(redisData));
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null){
            return Result.fail("店铺id不能为空");
        }
        //注意顺序 先更新（写）数据库 后删除缓存
        //1.更新数据库
        updateById(shop);
        //2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY+ id);
        return Result.ok();
    }
}
