package org.varun.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoWithShutdown {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        final Thread thread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run(){
                kafkaConsumer.wakeup();

                try {
                    thread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        try{
            kafkaConsumer.subscribe(Collections.singleton(""));
            while (true){
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                consumerRecords.forEach(r -> {
                    System.out.println(r.partition());
                    System.out.println(r.topic());
                });
            }
        }catch (WakeupException wakeupException){
            System.out.println(wakeupException);
        }catch (Exception e){

        }finally {
            kafkaConsumer.close();
        }
    }
}
