import org.apache.pulsar.client.api.*
import java.util.concurrent.TimeUnit
import kotlin.system.exitProcess

val ACK_TIMEOUT = 1000L;

fun main() {
    PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build()
        .use { client: PulsarClient ->
            client
                .newProducer(Schema.INT32)
                .topic("simple-demo")
                .create()
                .use { producer: Producer<Int> ->
                    (1..10).forEach { i ->
                        print("send msg [$i]...  ")
                        val msgId = producer.send(i)
                        println("ok, messageId = $msgId")
                    }

                }

            println("For now all messages are sent. Let's process them!")
            client.newConsumer(Schema.INT32)
                .topic("simple-demo")
                .ackTimeout(ACK_TIMEOUT, TimeUnit.MILLISECONDS)
                .subscriptionName("simple-subscription")
                .subscribe()
                .use { consumer: Consumer<Int> ->
                    var i = 0;
                    do {
                        val action = readAction()
                        val msg = consumer.receive()
                        when (action) {
                            Action.ACK -> {
                                consumer.acknowledge(msg.messageId)
                                println("send ack for msg: ${msg.value}, ${msg.messageId}")
                            }
                            Action.NEGATIVE_ACK -> {
                                consumer.negativeAcknowledge(msg.messageId)
                                println("send negative ack for msg: ${msg.value}, ${msg.messageId}")
                            }
                            Action.TIMEOUT -> {
                                Thread.sleep(ACK_TIMEOUT * 2)
                                println("sleep ${ACK_TIMEOUT * 2}ms and don't send any ack for msg: ${msg.value}, ${msg.messageId}")
                            }
                            Action.FAIL -> {
                                println("fail on msg: ${msg.value}, ${msg.messageId}")
                                println("Fail with status code 1, don't send any messages and commands to Pulsar")
                                exitProcess(1)
                            }
                        }
                        i++
                    } while (action != Action.EXIT && i < 10)
                }
        }
}

enum class Action(val cmd: String, val comment: String) {
    ACK("a", "Send standard acknowledge, that means 'I processed message, don't send me it again'"),
    NEGATIVE_ACK(
        "n",
        "Send negative asknowledge, that means 'I can't process message right now, give me another attempt'"
    ),
    TIMEOUT("t", "Don't send any acks for this message"),
    FAIL("f", "Terminate application (emulate kill -9)"),
    EXIT("e", "Exit from application")
}

fun readAction(): Action {
    var action: Action? = null;
    while (action == null) {
        print("What you want to do with next msg? [A]ck, [N]egativeAck, [T]imeout, [F]ail, [E]xit: ")
        val input = readLine();
        action = Action.values().find { it.cmd.equals(input, ignoreCase = true) }
        if (action == null) println("Can't recognize command, try again!")
    }

    return action!!
}