package com.hmdp.listener;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.MqConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Slf4j
@Component
public class SeckillOrderListener {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @RabbitListener(queues = MqConstants.SECKILL_ORDER_QUEUE)
    public void listenSeckillOrder(VoucherOrder voucherOrder) {
        log.info("Received seckill order message: {}", voucherOrder);
        voucherOrderService.handleVoucherOrder(voucherOrder);
    }
}
