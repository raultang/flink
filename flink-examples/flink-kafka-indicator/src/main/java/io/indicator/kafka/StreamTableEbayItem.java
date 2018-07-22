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
public class StreamTableEbayItem {

    private static final String[] fieldNames = {"marketplace", "id", "sale_type", "title", "location", "seller", "owner", "password", "category", "quantity", "bidcount", "created", "sale_start", "sale_end", "sale_status", "current_price", "start_price", "reserve_price", "high_bidder", "featured", "super_featured", "bold_title", "private_sale", "registered_only", "host", "visitcount", "picture_url", "last_modified", "icon_flags", "gallery_url", "gallery_thumb_x_size", "gallery_thumb_y_size", "gallery_state", "gallery_type", "country_id", "notice_time", "bill_time", "currency_id", "zip", "dutch_gms", "billing_currency_id", "shipping_option", "ship_region_flags", "desc_lang", "site_id", "maxbid", "payment_cash", "payment_moneyxfer", "payment_cc", "content_modified", "passwordex", "shipping_fee", "shipping_xfee", "tax_state", "return_policy", "photo_count", "photo_display_type", "tax_new", "tax_new_state", "sku_num", "category2", "bin_price", "relistid", "storefront_flags", "quantity_sold", "insurance_fee", "last_transaction_date", "myebay_delete", "end_code", "content_revised", "flags", "application_id", "customer_reference_code", "eoa_state", "eoa_retries", "cat_modified", "tuv_approval_code", "shipping_fee_2", "shipping_xfee_2", "shipping_fee_3", "shipping_xfee_3", "attr_label_id", "theme_id", "layout_id", "layout_flag", "paypal_seller_email", "origin_postal_code", "weight", "package_size", "weight_unit_type", "shipping_type", "paypal_icon_type", "current_winning_qty", "flags2", "gallery_image_source", "subtitle", "pbp_eligibility", "product_id", "lower_price_option", "relist_parent_id", "listing_method", "sale_sched_end", "flags3", "lot_size", "ppfo_offer_id", "qa_unanswered_question_count", "qa_public_count", "gallery_version", "utf8_status", "peak_photo_count2", "flags4", "peak_photo_count", "storefront_category2", "highest_best_offer_amt", "best_offer_count", "user_item_property_key", "user_designation", "reserved_quantity", "seller_product_id", "flags5", "update_version", "watch_count", "unique_bidder_count", "seller_delete_date", "unique_winner_count", "flags6", "flags7", "anonymous_highbidder_number", "flags08", "original_price", "promo_sale_start_date", "promo_sale_end_date", "ppl_running_balance_amt", "flags09", "flags10", "item_version_id", "significant_revise_count", "flags11", "flags12", "quantity_sold_by_pickup_store", "core_listing_version"};
    private static final TypeInformation[] fieldTypes = {Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING};

    private static Properties consumerProperties(){
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "rheos-db-stg-fcr-agg-kfk-3.lvs02.dev.ebayc3.com:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "caty-v2-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put("sasl.mechanism", "IAF");
		props.put("security.protocol", "SASL_PLAINTEXT");
		props.put("sasl.login.class", "io.ebay.rheos.kafka.security.iaf.IAFLogin");
		props.put("sasl.callback.handler.class", "io.ebay.rheos.kafka.security.iaf.IAFCallbackHandler");

        props.put("schema.registry.url", "http://rheossr-2458949.phx01.dev.ebayc3.com:8080");
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
            .forTopic("oradb.core4-fcr.caty.ebay-items")
            .forAvroRecordClass(EbayItemsRecord.class)
            .withKafkaProperties(consumerProperties())
            .withSchema(
            	//new TableSchema(fieldNames, fieldTypes))
                TableSchema.builder()
                    //.field("rheosHeader", Types.ROW(Types.LONG, Types.LONG, Types.INT, Types.STRING, Types.STRING))
                    .field("currentRecord", Types.ROW_NAMED(fieldNames, fieldTypes) )
                    .build())
            .build();

        tableEnv.registerTableSource("ebay_items", tableSource);

        //Table result = tableEnv.scan("sojevent").select("guid, eventTimestamp").where("eventTimestamp > 0");

        Table sqlResult  = tableEnv.sqlQuery("SELECT id, title FROM ebay_items");

        //TableSink tableSink = new CsvTableSink("/tmp/test","|", 1, WriteMode.NO_OVERWRITE);
        //tableEnv.registerTableSink("cvssink", fieldNames, fieldTypes, tableSink);


        tableEnv.toRetractStream(sqlResult, Row.class).print();
        //sqlResult.writeToSink(new CsvTableSink("/tmp/test", "|", 1, WriteMode.NO_OVERWRITE));

        //result.writeToSink(tableSink);

        env.execute();

    }

}
