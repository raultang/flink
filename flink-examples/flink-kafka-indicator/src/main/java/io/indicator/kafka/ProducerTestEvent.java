package io.indicator.kafka;

import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.flink.formats.avro.AvroRowSerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Created by ytang3 on 12/4/17.
 */
public class ProducerTestEvent {


    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "rhs-misc-crossdcs-stg2-kfk-1.lvs02.dev.ebayc3.com:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put("schema.registry.url", "http://rheossr-2458949.phx01.dev.ebayc3.com:8080");

        String topic = "misc.crossdcs.ty.test";

        Producer<byte[], byte[]> producer = new KafkaProducer<>(properties);

		KeyedSerializationSchemaWrapper<Row> schemaWrapper = new KeyedSerializationSchemaWrapper(new AvroRowSerializationSchema(TestAvroRecord.class));


		for (int i = 0; i < 10000; i++) {

			/*TestAvroRecord record = TestAvroRecord.newBuilder()
				.setGuid(UUID.randomUUID().toString())
				.setEventTimestamp(System.currentTimeMillis())
				.build();*/
			Row row = Row.of(UUID.randomUUID().toString(), System.currentTimeMillis());

            producer.send(new ProducerRecord<>(topic, ByteBuffer.allocate(4).putInt(i).array(), schemaWrapper.serializeValue(row)),
                    new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e != null) {
                                System.out.println("got error");
                            }

                            if (recordMetadata != null) {
                                System.out.println("message sent");
                            }
                        }
                    });

            TimeUnit.SECONDS.sleep(10);
        }

        producer.close();
    }
}
