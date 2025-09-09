package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    RedisIdWorker redisIdWorker;

    @Resource
    StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    @Override
    public Result seckillVoucher(Long voucherId) {
        //1.查询优惠券信息
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);

        //2.判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())){
            //尚未开始
            return Result.fail("秒杀尚未开始");
        }

        //3.判断秒杀是否已经结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())){
            return  Result.fail("秒杀已经结束");
        }

        //4.判断库存是否充足
        if (voucher.getStock()<1) {
            //库存不足
            return Result.fail("库存不足");
        }

        Long userId = UserHolder.getUser().getId();

        //使用reids分布式锁解决一人一单的并发问题
        //创建锁对象
        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId,stringRedisTemplate);
        RLock lock = redissonClient.getLock("lock:order:" + userId);


        //获取锁
        boolean isLock = lock.tryLock();

        //判断是否锁成功
        if (!isLock){
            //获取锁失败，返回错误或者重试
            return Result.fail("不允许重复下单");
        }

        try {
            synchronized (userId.toString().intern()) {  //这里intern() 是因为string对象每次都是new一个 为保证都是一个string的值 所以去看string的引用（字符串常量池）
                //原始对象导致的事务失效问题 获取动态代理解决
                IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
                return proxy.createVoucherOrder(voucherId);
            }
        } finally {
            //释放锁
        lock.unlock();
        }

    }

    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        //5.一人一单
        Long userId = UserHolder.getUser().getId();


            //5.1 查询订单
            Integer count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            //5.2 判断订单是否存在
            if (count > 0) {
                return Result.fail("用户已经购买过一次！");
            }

            //6.扣减库存(解决stock的并发问题)
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock -1") //set stock = stock -1
                    //where id = ? and stock >0  乐观锁
                    .eq("voucher_id", voucherId)
                    .gt("stock", 0)
                    .update();

            //判断是否执行成功
            if (!success) {
                //扣减失败
                return Result.fail("库存不足！");
            }


            //7.创建订单
            VoucherOrder voucherOrder = new VoucherOrder();

            //7.1 订单id
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);

            //7.2 用户id
            voucherOrder.setUserId(userId);

            //7.3 代金券id
            voucherOrder.setVoucherId(voucherId);

            //保存订单到数据库
            save(voucherOrder);

            //8.返回订单id
            return Result.ok(orderId);

    }
}
