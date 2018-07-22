package io.indicator.kafka;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;

/**
 * Created by ytang3 on 7/25/17.
 */
public class ConsumerTestEvent {

    private static String TOPIC_NAME = "misc.crossdcs.ty.test";
    private static String consumerGroup = "ty_test";
    private static boolean enableAuth = false;
    private static String coreServiceUrl = "https://rheos-services.qa.ebay.com";


    public static void main(String[] args) {
        testConsumer();
    }

    public static void testConsumer() {
        KafkaConsumer<byte[], EbayItemsRecord> consumer =  initConsumer();
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        while (true) {
            ConsumerRecords<byte[], EbayItemsRecord> consumerRecords = consumer.poll(Long.MAX_VALUE);
            for (ConsumerRecord<byte[], EbayItemsRecord> record : consumerRecords.records(TOPIC_NAME)) {
				System.out.println(record);
			}
        }
    }

    private static KafkaConsumer<byte[], EbayItemsRecord> initConsumer() {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "rhs-misc-crossdcs-stg2-kfk-1.lvs02.dev.ebayc3.com:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 64 * 1024);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1600 * 1024);
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());
		props.put("schema.registry.url", "http://rheossr-2458949.phx01.dev.ebayc3.com:8080");

        return new KafkaConsumer<>(props);
    }

}
