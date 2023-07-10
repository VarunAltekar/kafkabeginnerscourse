package org.varun.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        // create Producer properties
        Properties prop = new Properties();
        // to add properties to kafka producer, refer kafka documentation kafka.apache.org/documentation/#api

        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create Producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(prop);

        ProducerRecord<String, String> prec = new ProducerRecord<>("demo_topic", "message from java");

        // send data -
        kafkaProducer.send(prec);

        // after we execute the program, we see nothing on kafka-console-consumer
        // this happens because, data is sent asynchronously
        // so we need to do,
        kafkaProducer.flush();

        // close to release resource
        kafkaProducer.close();
    }
}
