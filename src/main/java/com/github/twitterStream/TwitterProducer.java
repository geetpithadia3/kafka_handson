package com.github.twitterStream;

import com.github.handson_kafka.ProducerDemoWithCallback;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    Logger log = LoggerFactory.getLogger(TwitterProducer.class);
    Integer tweetsToRead = 10;

    public static void main(String[] args) throws IOException {
        new TwitterProducer().run();
    }
    public TwitterProducer(){

    }
    public void run() throws IOException {
        // Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        //create a twitter client
        Client client =createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        client.connect();

        //create a kafka producer
        KafkaProducer<String,String> producer = createKafkaProducer();
        //loop to send tweets to kafka
        while (--tweetsToRead>=0 && !client.isDone()) {
            String msg= null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if(msg!=null){
                log.info(msg);
                producer.send(new ProducerRecord<>("twitter-tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e!=null)
                            log.error("Something went wrong!",e);
                    }
                });
            }

        }
        log.info("Shutting down client");
        client.stop();
        log.info("Closing Producer");
        producer.close();
        log.info("Good Bye!");
    }
    public Client createTwitterClient(BlockingQueue<String> msgQueue) throws IOException {


        //Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        ArrayList<String> terms = Lists.newArrayList("bitcoin");
        hosebirdEndpoint.trackTerms(terms);

        //Get the keys
        Properties properties = getTwitterConfig();
        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(properties.getProperty("consumerKey"),properties.getProperty("consumerSecret"),properties.getProperty("token"),properties.getProperty("tokenSecret"));

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));


        Client hosebirdClient = builder.build();
        return hosebirdClient;

    }

    public KafkaProducer<String,String> createKafkaProducer(){
        String bootstrapServers = "127.0.0.1:9092";
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        // create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        return producer;
    }

    public Properties getTwitterConfig() throws IOException {
        Properties properties = new Properties();
        String fileName="config.properties";
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(fileName);

        if(inputStream!= null)
            properties.load((inputStream));
        else
            throw new FileNotFoundException("Properties file not found");
        return properties;

    }

}
