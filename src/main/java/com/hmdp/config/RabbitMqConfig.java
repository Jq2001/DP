package com.hmdp.config;

import com.hmdp.utils.MqConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.rabbit.retry.RepublishMessageRecoverer;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.autoconfigure.amqp.RabbitTemplateCustomizer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.SECKILL_STOCK_KEY;

@Slf4j
@Configuration
@EnableRabbit
public class RabbitMqConfig {

    private static final int ORDER_PAY_TIMEOUT_MILLIS = (int) TimeUnit.MINUTES.toMillis(15);

    @Bean
    public DirectExchange seckillExchange() {
        return new DirectExchange(MqConstants.SECKILL_EXCHANGE, true, false);
    }

    @Bean
    public Queue seckillOrderQueue() {
        return QueueBuilder.durable(MqConstants.SECKILL_ORDER_QUEUE).build();
    }

    @Bean
    public Binding seckillOrderBinding(
            @Qualifier("seckillOrderQueue") Queue seckillOrderQueue,
            @Qualifier("seckillExchange") DirectExchange seckillExchange) {
        return BindingBuilder.bind(seckillOrderQueue)
                .to(seckillExchange)
                .with(MqConstants.SECKILL_ORDER_ROUTING_KEY);
    }

    @Bean
    public DirectExchange orderDelayExchange() {
        return new DirectExchange(MqConstants.ORDER_DELAY_EXCHANGE, true, false);
    }

    @Bean
    public Queue orderDelayQueue() {
        return QueueBuilder.durable(MqConstants.ORDER_DELAY_QUEUE)
                .ttl(ORDER_PAY_TIMEOUT_MILLIS)
                .deadLetterExchange(MqConstants.ORDER_DEAD_EXCHANGE)
                .deadLetterRoutingKey(MqConstants.ORDER_TIMEOUT_ROUTING_KEY)
                .build();
    }

    @Bean
    public Binding orderDelayBinding(
            @Qualifier("orderDelayQueue") Queue orderDelayQueue,
            @Qualifier("orderDelayExchange") DirectExchange orderDelayExchange) {
        return BindingBuilder.bind(orderDelayQueue)
                .to(orderDelayExchange)
                .with(MqConstants.ORDER_DELAY_ROUTING_KEY);
    }

    @Bean
    public DirectExchange orderDeadExchange() {
        return new DirectExchange(MqConstants.ORDER_DEAD_EXCHANGE, true, false);
    }

    @Bean
    public Queue orderTimeoutQueue() {
        return QueueBuilder.durable(MqConstants.ORDER_TIMEOUT_QUEUE).build();
    }

    @Bean
    public Binding orderTimeoutBinding(
            @Qualifier("orderTimeoutQueue") Queue orderTimeoutQueue,
            @Qualifier("orderDeadExchange") DirectExchange orderDeadExchange) {
        return BindingBuilder.bind(orderTimeoutQueue)
                .to(orderDeadExchange)
                .with(MqConstants.ORDER_TIMEOUT_ROUTING_KEY);
    }

    @Bean
    public DirectExchange errorExchange() {
        return new DirectExchange(MqConstants.ERROR_EXCHANGE, true, false);
    }

    @Bean
    public Queue errorQueue() {
        return QueueBuilder.durable(MqConstants.ERROR_QUEUE).build();
    }

    @Bean
    public Binding errorBinding(
            @Qualifier("errorQueue") Queue errorQueue,
            @Qualifier("errorExchange") DirectExchange errorExchange) {
        return BindingBuilder.bind(errorQueue)
                .to(errorExchange)
                .with(MqConstants.ERROR_ROUTING_KEY);
    }

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public RabbitTemplateCustomizer rabbitTemplateCustomizer(StringRedisTemplate stringRedisTemplate) {
        return rabbitTemplate -> {
            rabbitTemplate.setMandatory(true);
            rabbitTemplate.setReturnCallback((message, replyCode, replyText, exchange, routingKey) -> {
                rollbackReturnedSeckillMessage(message.getMessageProperties().getHeaders(), stringRedisTemplate);
                log.error(
                        "RabbitMQ message returned, exchange: {}, routingKey: {}, replyCode: {}, replyText: {}",
                        exchange,
                        routingKey,
                        replyCode,
                        replyText
                );
            });
        };
    }

    private void rollbackReturnedSeckillMessage(Map<String, Object> headers, StringRedisTemplate stringRedisTemplate) {
        Object voucherId = headers.get("voucherId");
        Object userId = headers.get("userId");
        if (voucherId == null || userId == null) {
            return;
        }
        stringRedisTemplate.opsForValue().increment(SECKILL_STOCK_KEY + voucherId);
        stringRedisTemplate.opsForSet().remove("seckill:order:" + voucherId, userId.toString());
    }

    @Bean
    public MessageRecoverer messageRecoverer(RabbitTemplate rabbitTemplate) {
        return new RepublishMessageRecoverer(
                rabbitTemplate,
                MqConstants.ERROR_EXCHANGE,
                MqConstants.ERROR_ROUTING_KEY
        );
    }
}
