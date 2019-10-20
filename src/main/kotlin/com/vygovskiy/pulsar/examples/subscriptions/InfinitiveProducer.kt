package com.vygovskiy.pulsar.examples.subscriptions

import com.vygovskiy.pulsar.examples.ACK_TIMEOUT
import com.vygovskiy.pulsar.examples.Action
import com.vygovskiy.pulsar.examples.readAction
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.Schema
import java.util.concurrent.TimeUnit
import kotlin.random.Random
import kotlin.system.exitProcess

val TOPIC = "subscriptions-demo"

fun main() {
    PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build()
        .use { client: PulsarClient ->
            client
                .newProducer(Schema.INT32)
                .topic(TOPIC)
                .create()
                .use { producer: Producer<Int> ->
                    while (true) {
                        println("Input key ([e]xit for exit): ")
                        val key: String = readLine()!!
                        if ("e" == key) break;

                        val value = Random.nextInt(0,1000)
                        val messageId = producer
                            .newMessage()
                            .key(key)
                            .value(value)
                            .send()

                        println("sent message: id = ${messageId}, key = ${key}, value = ${value}")
                    }
                }
        }
}