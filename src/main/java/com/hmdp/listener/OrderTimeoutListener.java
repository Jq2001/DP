package com.hmdp.listener;

import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.MqConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Slf4j
@Component
public class OrderTimeoutListener {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @RabbitListener(queues = MqConstants.ORDER_TIMEOUT_QUEUE)
    public void listenOrderTimeout(Long orderId) {
        log.info("Received order timeout message, orderId: {}", orderId);
        voucherOrderService.cancelTimeoutOrder(orderId);
    }
}
