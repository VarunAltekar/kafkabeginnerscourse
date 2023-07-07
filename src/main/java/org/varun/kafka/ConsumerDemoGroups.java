package org.varun.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoGroups {
    /*
        By changing group id config you can reset an application to start reading from beginning
        We can achieve the same thing by resetting the offset
        kafka-consumer-groups --bootstrap-server localhost:9092 --topic first_topic
            --group my-fifth-app --reset-offsets --to-offset 0 --execute

        Rebalances -

        Start 3 consoles (since we have 3 partition)for this program and check logs for
            AbstractCoordinator & ConsumerCoordinator
        [Revoking previously assigned partitions
         Rejoining group
         assigning and reassigning partitions]


     */
    public static void main(String[] args) {

        // props
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-fifth-app");
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

        // subscribe to topic
        consumer.subscribe(Collections.singleton("first_topic"));

        // poll
        while (true){
            ConsumerRecords<String, String> rec = consumer.poll(Duration.ofMillis(100));
            rec.forEach( r -> {
                System.out.println(r.topic());
                System.out.println(r.value());
                System.out.println(r.partition());
            });
        }

    }
}
