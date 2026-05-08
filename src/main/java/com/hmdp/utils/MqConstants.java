package com.hmdp.utils;

public class MqConstants {

    public static final String SECKILL_EXCHANGE = "seckill.direct";
    public static final String SECKILL_ORDER_QUEUE = "seckill.order.queue";
    public static final String SECKILL_ORDER_ROUTING_KEY = "seckill.order";

    public static final String ERROR_EXCHANGE = "error.direct";
    public static final String ERROR_QUEUE = "error.queue";
    public static final String ERROR_ROUTING_KEY = "error";

    public static final String ORDER_DELAY_EXCHANGE = "order.delay.direct";
    public static final String ORDER_DELAY_QUEUE = "order.delay.queue";
    public static final String ORDER_DELAY_ROUTING_KEY = "order.delay";

    public static final String ORDER_DEAD_EXCHANGE = "order.dead.direct";
    public static final String ORDER_TIMEOUT_QUEUE = "order.timeout.queue";
    public static final String ORDER_TIMEOUT_ROUTING_KEY = "order.timeout";

    private MqConstants() {
    }
}
