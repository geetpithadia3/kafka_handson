package com.github.handson_kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKey {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String bootstrapServers = "127.0.0.1:9092";
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithKey.class);

        // create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0;i<10;i++){
            String topic = "java-topic";
            String value = "Hello World: "+ i;
            String key = "id_"+ i;
            // create a ProducerRecord
            ProducerRecord<String,String> record = new ProducerRecord<String,String>(topic,key,value);

            logger.info("Key:"+key);
            // id_0: P1
            // id_1: P0
            // id_2: P2
            // id_3: P0
            // id_4: P2
            // id_5: P2
            // id_6: P0
            // id_7: P2
            // id_8: P1
            // id_9: P2

            // send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes everytime the record is successfully sent or an exception is thrown
                    if(e== null){
                        logger.info("Received new metadata. \n"+
                                "Topic:"+recordMetadata.topic()+"\n"+
                                "Partition:"+recordMetadata.partition()+"\n"+
                                "Offset:"+recordMetadata.offset()+"\n"+
                                "Timestamp:"+recordMetadata.timestamp());
                    }
                    else{

                    }
                }
            }).get(); //making it sync
        }
        producer.flush();
        producer.close();
    }
}
