package com.vygovskiy.pulsar.examples

import org.apache.pulsar.client.api.*
import java.util.*

fun main() {
    PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build()
        .use { client: PulsarClient ->
            client
                .newProducer(Schema.STRING)
                .topic("simple-demo")
                .create()
                .use { producer: Producer<String> ->
                    val uuid = UUID.randomUUID().toString()
                    val msg = "[${uuid}] Hello, Pulsar!"
                    print("send msg $msg")
                    val msgId = producer.send(msg)

                    println("ok, messageId = ${msgId}")
                }

            client.newConsumer(Schema.STRING)
                .topic("simple-demo")
                .subscriptionName("simple-subscription")
                .subscribe()
                .use { consumer : Consumer<String> ->
                    println("receiving... ")
                    val msg: Message<String> = consumer.receive()
                    try {
                        println("Message received: id = ${msg.messageId}, value = ${msg.data.toString(Charsets.UTF_8)}")
                        consumer.acknowledge(msg.messageId)
                    } catch (e: Exception) {
                        consumer.negativeAcknowledge(msg.messageId)
                    }
                }
        }


}