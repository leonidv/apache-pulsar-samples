package com.vygovskiy.pulsar.examples.subscriptions

import org.apache.pulsar.client.api.*

/**
 * Execute simple INT32 consumer which just println messages in infinitive loop
 */
fun executeSimpleConsumer(topic: String,  subscriptionName : String, subscriptionType: SubscriptionType) {
    println("topic = $topic, subscriptionName : $subscriptionName, subscriptionType: $subscriptionType")
    PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build()
        .use { client: PulsarClient ->
            client.newConsumer(Schema.INT32)
                .topic(topic)
                .subscriptionName(subscriptionName)
                .subscriptionType(subscriptionType)
                .subscribe()
                .use { consumer : Consumer<Int> ->
                    while (true) {
                        println("receiving... ")
                        val msg: Message<Int> = consumer.receive()
                        try {
                            println("Message received: id = ${msg.messageId}, key = ${msg.key}, value = ${msg.value}")
                            consumer.acknowledge(msg.messageId)
                        } catch (e: Exception) {
                            consumer.negativeAcknowledge(msg.messageId)
                        }
                    }
                }
        }
}