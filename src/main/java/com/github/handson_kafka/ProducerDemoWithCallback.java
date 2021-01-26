package com.github.handson_kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        // create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0;i<10;i++){
            // create a ProducerRecord
            ProducerRecord<String,String> record = new ProducerRecord<String,String>("java-topic","Hello World: "+Integer.toString(i));

            // send Data
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
            });
        }
        producer.flush();
        producer.close();
    }
}
