package com.vygovskiy.pulsar.examples.subscriptions

import org.apache.pulsar.client.api.SubscriptionType

fun main() {
    executeSimpleConsumer(TOPIC, "failover-subscription", SubscriptionType.Failover)
}