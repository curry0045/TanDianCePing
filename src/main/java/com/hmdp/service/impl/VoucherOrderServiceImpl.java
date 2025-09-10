package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
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

    private IVoucherOrderService proxy;

    //private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);

    private static final ExecutorService SECKILL_ORDER_HANDLER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct //让当前类初始化之后 就执行这个任务（生命周期）
    private void init() {
        SECKILL_ORDER_HANDLER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {
        String queueName = "stream.orders";

        @Override
        public void run() {
            while (true) {
                try {
                    //1 获取消息队列中的订单信息  XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
                    //这里MapRecord<String, Object, Object> 第一个string是消息队列的名称 第二个Object表示消息中的键（field） 第三个第二个Object表示消息中的值（value）
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );

                    //2 判断消息是否获取成功
                    if (list == null || list.isEmpty()) {
                        //2.1 如果获取失败，说明没有消息，继续下一次循环
                        continue;
                    }

                    // 如果获取成功，可以下单
                    //3 解析消息中的订单信息 一般只有1个消息 所以取下标0的位置信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();

                    //4封装成VoucherOrder
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                    //5 如果获取成功 可以下单
                    handleVoucherOrder(voucherOrder);

                    //6 ACK确认 XACK
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());

                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    //进入pending-list去处理没有ack的消息
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    //1 获取pendinglist中的订单信息  XREADGROUP GROUP g1 c1 COUNT 1 STREAMS stream.orders 0
                    //这里MapRecord<String, Object, Object> 第一个string是消息队列的名称 第二个Object表示消息中的键（field） 第三个第二个Object表示消息中的值（value）
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );

                    //2 判断消息是否获取成功
                    if (list == null || list.isEmpty()) {
                        //2.1 如果获取失败，说明pendinglist没有消息，结束循环
                        break;
                    }

                    // 如果获取成功，可以下单
                    //3 解析消息中的订单信息 一般只有1个消息 所以取下标0的位置信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();

                    //4封装成VoucherOrder
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                    //5 如果获取成功 可以下单
                    handleVoucherOrder(voucherOrder);

                    //6 ACK确认 XACK
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());

                } catch (Exception e) {
                    log.error("处理pending-list订单异常", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    }
//    private class VoucherOrderHandler implements Runnable{
//
//        @Override
//        public void run() {
//            try {
//                //1 获取队列中的订单信息
//                VoucherOrder voucherOrder = orderTasks.take();
//                //2 创建订单
//                handleVoucherOrder(voucherOrder);
//            } catch (Exception e) {
//                log.error("处理订单异常",e);
//            }
//        }
//    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        //获取用户
        Long userId = voucherOrder.getUserId();
        //创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);

        //获取锁
        boolean isLock = lock.tryLock();

        //判断是否锁成功
        if (!isLock) {
            //获取锁失败，返回错误
            log.error("不允许重复下单");
        }

        try {
            synchronized (userId.toString().intern()) {  //这里intern() 是因为string对象每次都是new一个 为保证都是一个string的值 所以去看string的引用（字符串常量池）
                //子线程 不是父线程 无法通过动态代理 所以直接使用局部变量
                proxy.createVoucherOrder(voucherOrder);
            }
        } finally {
            //释放锁
            lock.unlock();
        }
    }

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        UserDTO user = UserHolder.getUser();
        Long userId = user.getId();
        //获取订单id
        long orderId = redisIdWorker.nextId("order");
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );

        //3.主线程获取代理对象 子线程直接拿
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //4.返回订单id

        return Result.ok(orderId);

    }

//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //获取用户
//        UserDTO user = UserHolder.getUser();
//        Long userId = user.getId();
//        //1.执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString()
//        );
//
//        //2.判断结果是否为0
//        int r = result.intValue();
//        if (r != 0) {
//            //2.1 不为0 代表没有购买资格
//            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
//        }
//
//        //2.2 为0，有购买资格，把下单信息保存到阻塞队列
//        VoucherOrder voucherOrder = new VoucherOrder();
//        //2.3订单id
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        //2.4用户id
//        voucherOrder.setUserId(userId);
//        //2.5代金券id
//        voucherOrder.setVoucherId(voucherId);
//        //2.6放入阻塞队列
//        orderTasks.add(voucherOrder);
//
//        //3.主线程获取代理对象
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//        //4.返回订单id
//
//        return  Result.ok(orderId);
//
//    }

//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //1.查询优惠券信息
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//
//        //2.判断秒杀是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())){
//            //尚未开始
//            return Result.fail("秒杀尚未开始");
//        }
//
//        //3.判断秒杀是否已经结束
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())){
//            return  Result.fail("秒杀已经结束");
//        }
//
//        //4.判断库存是否充足
//        if (voucher.getStock()<1) {
//            //库存不足
//            return Result.fail("库存不足");
//        }
//
//        Long userId = UserHolder.getUser().getId();
//
//        //使用reids分布式锁解决一人一单的并发问题
//        //创建锁对象
//        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId,stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//
//
//        //获取锁
//        boolean isLock = lock.tryLock();
//
//        //判断是否锁成功
//        if (!isLock){
//            //获取锁失败，返回错误或者重试
//            return Result.fail("不允许重复下单");
//        }
//
//        try {
//            synchronized (userId.toString().intern()) {  //这里intern() 是因为string对象每次都是new一个 为保证都是一个string的值 所以去看string的引用（字符串常量池）
//                //原始对象导致的事务失效问题 获取动态代理解决
//                IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//                return proxy.createVoucherOrder(voucherId);
//            }
//        } finally {
//            //释放锁
//        lock.unlock();
//        }
//
//    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //5.一人一单
        Long userId = voucherOrder.getUserId();


        //5.1 查询订单
        Integer count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        //5.2 判断订单是否存在
        if (count > 0) {
            log.error("用户已经购买过一次！");
        }

        //6.扣减库存(解决stock的并发问题)
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock -1") //set stock = stock -1
                //where id = ? and stock >0  乐观锁
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0)
                .update();

        //判断是否执行成功
        if (!success) {
            //扣减失败
            log.error("库存不足！");
        }


        //保存订单到数据库
        save(voucherOrder);


    }
}
