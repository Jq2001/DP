package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.redisson.api.RedissonClient;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;

/**
 * 服务实现类
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService iSeckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户id
        Long userId = UserHolder.getUser().getId();
        //执行lua脚本
        Long lresult = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString()
        );
        //判断返回是否为0
        int result = lresult.intValue();
        if (result != 0) {
            //不为0 无法购买
            return Result.fail(result == 1?"库存不足":"不能重复下单");
        }
        //为0 可以购买 把下单信息保存到阻塞队列
        long orderId = redisIdWorker.nextId("order");
        //返回订单id
        return Result.ok(orderId);
    }

    @Override
    public Result createVoucherOrder(Long voucherId) {
        return null;
    }
}
//        //查询优惠卷
//        SeckillVoucher voucher = iSeckillVoucherService.getById(voucherId);
//        //判断是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            //未开始
//            return Result.fail("秒杀尚未开始!");
//        }
//        //判断是否结束
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            //未开始
//            return Result.fail("秒杀已经结束!");
//        }
//        //判断库存是否充足
//        if (voucher.getStock() < 1) {
//            return Result.fail("库存不足!");
//        }
//        Long userID = UserHolder.getUser().getId();
////        synchronized (userID.toString().intern()) {
//// 获取代理对象(事务)
////            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
////            return proxy.createVoucherOrder(voucherId);
//
//        //创建锁对象
////        SimpleRedisLock lock = new SimpleRedisLock("order" + userID, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + userID);
//        boolean isLock = lock.tryLock();
//        if (!isLock) {
//            //获取锁失败 返回错误信息或者重试
//            return Result.fail("禁止重复下单!");
//        }
//        try {
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } catch (IllegalStateException e) {
//            throw new RuntimeException(e);
//        } finally {
//            lock.unlock();
//        }
////        }

//    @Transactional
//    public Result createVoucherOrder(Long voucherId) {
//        //一人一单
//        Long userID = UserHolder.getUser().getId();
//        int count = query().eq("user_id", userID).eq("voucher_id", voucherId).count();
//        //查询订单
//        // 判断是否存在
//        if (count > 0) {
//            return Result.fail("用户已经购买过了");
//        }
//        //扣库存
//        boolean ifSuccess = iSeckillVoucherService.update().setSql("stock = stock -1").eq("voucher_id", voucherId).gt("stock", 0).update();
//        //扣减失败
//        if (!ifSuccess) {
//            return Result.fail("库存不足!");
//        }
//        //创建订单
//        VoucherOrder voucherOrder = new VoucherOrder();
//        //订单 id
//        long orderID = redisIdWorker.nextId("Order");
//        voucherOrder.setId(orderID);
//        //用户 id
//        voucherOrder.setUserId(userID);
//        //代金券 id
//        voucherOrder.setVoucherId(voucherId);
//
//        save(voucherOrder);
//        return Result.ok(orderID);
//    }

