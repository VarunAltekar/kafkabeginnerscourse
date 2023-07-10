package org.varun.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerStickyPartition {
    public static void main(String[] args) {
        // 1. create properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "400");

        // 2. instantiate kafka producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        // 6 batches with 6 records per batch
        // 2 batch will go to same partition
        for(int j=0; j<=5; j++) {
            for (int i = 0; i <= 5; i++) {
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>("sticky_partitioner", "sticky partiontion - " + i);
                kafkaProducer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            System.out.println("record posted to topic : " + recordMetadata.topic());
                            System.out.println("record posted to partition : " + recordMetadata.partition());
                            System.out.println("partition offset : " + recordMetadata.offset());
                        } else {
                            e.printStackTrace();
                        }
                    }
                });
            }
        }
        kafkaProducer.flush();
        kafkaProducer.close();

    }
}
