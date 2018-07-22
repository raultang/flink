/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.indicator.kafka;

import java.util.Properties;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka010AvroTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;

/**
 * Created by ytang3 on 6/11/18.
 */
public class StreamTableSojTotal {

    private static final String[] fieldNames = {"guid", "eventTimestamp"};
    private static final TypeInformation[] fieldTypes = {Types.STRING, Types.LONG};

    private static Properties consumerProperties(){
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "rheos-bh-stg-agg-kfk-1.lvs02.dev.ebayc3.com:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ty_test");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("schema.registry.url", "http://rheossr-2458949.phx01.dev.ebayc3.com:8080");
        //if(enableAuth) {
        //    props.put("sasl.mechanism", "IAF");
        //    props.put("security.protocol", "SASL_PLAINTEXT");
        //    props.put("sasl.login.class", "io.ebay.rheos.kafka.security.iaf.IAFLogin");
        //    props.put("sasl.callback.handler.class", "io.ebay.rheos.kafka.security.iaf.IAFCallbackHandler");
        //}
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 64 * 1024);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1600 * 1024);
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());

        return props;
    }


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        KafkaTableSource tableSource = Kafka010AvroTableSource.builder()
            .forTopic("behavior.pulsar.sojevent.total")
            .forAvroRecordClass(SojEvent.class)
            .withKafkaProperties(consumerProperties())
            .withSchema(
                TableSchema.builder()
                    .field("guid", Types.STRING)
                    .field("eventTimestamp", Types.LONG)
					.field("pageId", Types.INT)
                    .build())
			.withKafkaTimestampAsRowtimeAttribute()
            .build();

        tableEnv.registerTableSource("sojevent", tableSource);

        //Table result = tableEnv.scan("sojevent").select("guid, eventTimestamp").where("eventTimestamp > 0");

        Table sqlResult  = tableEnv.sqlQuery("SELECT count(1) FROM sojevent");

        //TableSink tableSink = new CsvTableSink("/tmp/test","|", 1, WriteMode.NO_OVERWRITE);
        //tableEnv.registerTableSink("cvssink", fieldNames, fieldTypes, tableSink);


        tableEnv.toRetractStream(sqlResult, Row.class).print();
        //sqlResult.writeToSink(new CsvTableSink("/tmp/test", "|", 1, WriteMode.NO_OVERWRITE));

        //result.writeToSink(tableSink);

        env.execute();

    }

}
