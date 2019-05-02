package com.aygavras.kafkastreaming

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.state.KeyValueStore
import java.util.*
import java.util.Locale
import java.util.concurrent.CountDownLatch


//@SpringBootApplication
class KafkaStreamingApplication

fun main(args: Array<String>) {


    val inputTopic = "inputTopic"
    val tempDirectory = "/tmp/mukhisar/kafka" // configure

    val streamsConfiguration = Properties()
    streamsConfiguration.put(
        StreamsConfig.APPLICATION_ID_CONFIG,
        "wordcount-live-test"
    );

    val bootstrapServers = "localhost:9093,localhost:9094,localhost:9095"

    streamsConfiguration[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers



    streamsConfiguration.put(
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        Serdes.String().javaClass.name
    )
    streamsConfiguration.put(
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        Serdes.String().javaClass.name
    )

    streamsConfiguration.put(
        StreamsConfig.STATE_DIR_CONFIG,
        tempDirectory
    )
    //   runApplication<KafkaStreamingApplication>(*args)

    val builder = StreamsBuilder()
    val source: KStream<String, String> = builder.stream(inputTopic)



    source
        .flatMapValues { value: String -> value.toLowerCase(Locale.getDefault()).split("\\W+".toRegex())}
        .groupBy { _, word -> word}
        .count(Materialized.`as`<String, Long, KeyValueStore<Bytes, ByteArray>>("counts-store"))
        //   .count(Materialized.`as`<String>, Long, KeyValueStore<Bytes, ByteArray>>("counts-store"))
        .toStream()
        .to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()))

    // source.to("outputTopic")


    val topology = builder.build()

    print(topology.describe())
    val streams = KafkaStreams(topology, streamsConfiguration)

    val latch = CountDownLatch(1)

    // attach shutdown handler to catch control-c

    Runtime.getRuntime().addShutdownHook(object : Thread("streams-wordcount-shutdown-hook") {
        override fun run() {
            streams.close()
            latch.countDown()
        }
    })
    try {
        streams.start()
        latch.await()
    } catch (e: Throwable) {
        System.exit(1)
    }
    System.exit(0)
}

