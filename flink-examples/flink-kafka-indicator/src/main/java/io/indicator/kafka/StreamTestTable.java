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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Created by ytang3 on 6/11/18.
 */
public class StreamTestTable {

    private static final String[] fieldNames = {"guid", "eventTimestamp"};
    private static final TypeInformation[] fieldTypes = {Types.STRING, Types.LONG};


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        StreamTestSource testSource = new StreamTestSource();

        tableEnv.registerTableSource("sojevent", testSource);

        //Table result = tableEnv.scan("sojevent").select("guid, eventTimestamp").where("eventTimestamp > 0");

        Table sqlResult  = tableEnv.sqlQuery("SELECT * FROM sojevent");

        //TableSink tableSink = new CsvTableSink("/tmp/test","|", 1, WriteMode.NO_OVERWRITE);
        //tableEnv.registerTableSink("cvssink", fieldNames, fieldTypes, tableSink);


        tableEnv.toRetractStream(sqlResult, Row.class).print();
        //sqlResult.writeToSink(new CsvTableSink("/tmp/test", "|", 1, WriteMode.OVERWRITE));

        //result.writeToSink(tableSink);

        env.execute();

    }

}
