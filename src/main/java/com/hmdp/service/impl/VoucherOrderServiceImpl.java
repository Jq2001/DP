package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.MqConstants;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.SECKILL_STOCK_KEY;

@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    private static final int ORDER_STATUS_UNPAID = 1;
    private static final int ORDER_STATUS_CANCELED = 4;

    @Resource
    private ISeckillVoucherService iSeckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private RabbitTemplate rabbitTemplate;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("Order");

        Long luaResult = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString()
        );

        int result = luaResult == null ? 1 : luaResult.intValue();
        if (result != 0) {
            return Result.fail(result == 1 ? "库存不足" : "不能重复下单");
        }

        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
        voucherOrder.setStatus(ORDER_STATUS_UNPAID);

        if (!sendSeckillOrderMessage(voucherOrder, voucherId, userId)) {
            return Result.fail("下单失败，请稍后重试");
        }

        return Result.ok(orderId);
    }

    private boolean sendSeckillOrderMessage(VoucherOrder voucherOrder, Long voucherId, Long userId) {
        CorrelationData correlationData = new CorrelationData(voucherOrder.getId().toString());
        try {
            rabbitTemplate.convertAndSend(
                    MqConstants.SECKILL_EXCHANGE,
                    MqConstants.SECKILL_ORDER_ROUTING_KEY,
                    voucherOrder,
                    message -> {
                        message.getMessageProperties().setHeader("voucherId", voucherId);
                        message.getMessageProperties().setHeader("userId", userId);
                        return message;
                    },
                    correlationData
            );
            CorrelationData.Confirm confirm = correlationData.getFuture().get(3, TimeUnit.SECONDS);
            if (confirm == null || !confirm.isAck()) {
                rollbackRedisSeckillState(voucherId, userId);
                log.error("Failed to confirm seckill order message, orderId: {}, reason: {}",
                        voucherOrder.getId(),
                        confirm == null ? "confirm timeout" : confirm.getReason());
                return false;
            }
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            rollbackRedisSeckillState(voucherId, userId);
            log.error("Interrupted while waiting for seckill order message confirm", e);
            return false;
        } catch (Exception e) {
            rollbackRedisSeckillState(voucherId, userId);
            log.error("Failed to publish seckill order message", e);
            return false;
        }
    }

    private void rollbackRedisSeckillState(Long voucherId, Long userId) {
        stringRedisTemplate.opsForValue().increment(SECKILL_STOCK_KEY + voucherId);
        stringRedisTemplate.opsForSet().remove("seckill:order:" + voucherId, userId.toString());
    }

    @Override
    public void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = lock.tryLock();
        if (!isLock) {
            log.error("Duplicate seckill order is not allowed, userId: {}", userId);
            return;
        }
        try {
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }

    @Override
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();

        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0) {
            log.error("User has already bought this voucher, userId: {}, voucherId: {}", userId, voucherId);
            return;
        }

        boolean success = iSeckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();
        if (!success) {
            log.error("Voucher stock is not enough, voucherId: {}", voucherId);
            return;
        }

        save(voucherOrder);
        rabbitTemplate.convertAndSend(
                MqConstants.ORDER_DELAY_EXCHANGE,
                MqConstants.ORDER_DELAY_ROUTING_KEY,
                voucherOrder.getId()
        );
    }

    @Override
    @Transactional
    public void cancelTimeoutOrder(Long orderId) {
        VoucherOrder voucherOrder = getById(orderId);
        if (voucherOrder == null) {
            log.warn("Timeout order does not exist, orderId: {}", orderId);
            return;
        }

        if (!Integer.valueOf(ORDER_STATUS_UNPAID).equals(voucherOrder.getStatus())) {
            log.info("Timeout order has been handled, orderId: {}, status: {}", orderId, voucherOrder.getStatus());
            return;
        }

        boolean cancelSuccess = update()
                .set("status", ORDER_STATUS_CANCELED)
                .eq("id", orderId)
                .eq("status", ORDER_STATUS_UNPAID)
                .update();
        if (!cancelSuccess) {
            log.info("Timeout order cancel skipped, orderId: {}", orderId);
            return;
        }

        Long voucherId = voucherOrder.getVoucherId();
        Long userId = voucherOrder.getUserId();

        boolean restoreDbStockSuccess = iSeckillVoucherService.update()
                .setSql("stock = stock + 1")
                .eq("voucher_id", voucherId)
                .update();
        if (!restoreDbStockSuccess) {
            throw new IllegalStateException("Failed to restore voucher stock, voucherId: " + voucherId);
        }

        stringRedisTemplate.opsForValue().increment(SECKILL_STOCK_KEY + voucherId);
        stringRedisTemplate.opsForSet().remove("seckill:order:" + voucherId, userId.toString());
        log.info("Timeout order canceled, orderId: {}, voucherId: {}, userId: {}", orderId, voucherId, userId);
    }
}
