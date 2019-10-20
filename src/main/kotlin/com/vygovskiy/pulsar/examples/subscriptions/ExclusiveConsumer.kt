package com.vygovskiy.pulsar.examples.subscriptions

import org.apache.pulsar.client.api.*
import java.util.*

fun main() {
    executeSimpleConsumer(TOPIC, "exclusive-subscription3", SubscriptionType.Exclusive)
}