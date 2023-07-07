package org.varun.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class ProducerWithKey {

    public static void main(String[] args) {

        // create properties
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // instantiate producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(prop);

        Random random = new Random();

        /*
            The idea here is to see that same key goes in same partition, during an iteration
         */
        for(int i=0; i<=2; i++){
            // instantiate record
            int key = random.nextInt(2);
            System.out.println(key);
            ProducerRecord<String,String> record = new ProducerRecord<>("first_topic", String.valueOf( "id_"+ key),"Record with key");
            // send - we are using get() to make it synchronous, i.e. to block send()
            try {
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
                }).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            //flush
            producer.flush();

        }
        producer.close();

    }
}
