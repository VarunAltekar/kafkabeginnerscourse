package org.varun.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerCallbackDemo {

    public static void main(String[] args) {

        // create properties
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // instantiate kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
        // instantiate kafka record
        ProducerRecord<String,String> record = new ProducerRecord<String,String>("callback_demo_topic","Callback Record");
        // send on the topic
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                if(e == null){
                    System.out.println( "record posted to topic : " + recordMetadata.topic());
                    System.out.println( "record posted to partition : " + recordMetadata.partition());
                    System.out.println( "record posted to topic : " + recordMetadata.offset());
                }
                else{
                    e.printStackTrace();
                }
            }
        });

        producer.flush();
        producer.close();
    }
}
