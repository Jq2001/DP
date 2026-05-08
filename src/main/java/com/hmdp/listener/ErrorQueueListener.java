package com.hmdp.listener;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.MqConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Slf4j
@Component
public class ErrorQueueListener {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private MessageConverter messageConverter;

    @RabbitListener(queues = MqConstants.ERROR_QUEUE)
    public void listenErrorMessage(Message message) {
        Object payload;
        try {
            payload = messageConverter.fromMessage(message);
        } catch (Exception e) {
            log.error("Failed to convert error queue message, messageId: {}",
                    message.getMessageProperties().getMessageId(), e);
            return;
        }

        if (!(payload instanceof VoucherOrder)) {
            log.error("Unhandled error queue message payload: {}", payload);
            return;
        }

        VoucherOrder voucherOrder = (VoucherOrder) payload;
        if (voucherOrder.getId() == null
                || voucherOrder.getVoucherId() == null
                || voucherOrder.getUserId() == null) {
            log.error("Invalid seckill error message payload: {}", voucherOrder);
            return;
        }

        if (voucherOrderService.getById(voucherOrder.getId()) != null) {
            log.error("Seckill order message failed after order was created, orderId: {}",
                    voucherOrder.getId());
            return;
        }

        voucherOrderService.rollbackRedisSeckillState(
                voucherOrder.getVoucherId(),
                voucherOrder.getUserId()
        );
        log.error("Seckill order message failed finally, rollback redis state, order: {}", voucherOrder);
    }
}
