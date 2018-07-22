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

import java.util.UUID;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

/**
 * Created by ytang3 on 6/18/18.
 */
public class StreamTestSource implements StreamTableSource<Row> {

	private static final String[] fieldNames = {"guid", "eventTimestamp"};
	private static final TypeInformation[] fieldTypes = {Types.STRING, Types.LONG};

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		DataStream<Row> dataStream = execEnv.addSource(new SourceFunction<Row>() {
			private long count = 0L;
			private volatile boolean isRunning = true;
			@Override
			public void run(SourceContext<Row> ctx) throws Exception {
				while (isRunning && count < 10){
					synchronized (ctx.getCheckpointLock()){
						Row row = new Row(2);
						row.setField(0, UUID.randomUUID().toString());
						row.setField(1, System.currentTimeMillis());
						ctx.collect(row);
						count++;
					}
				}
			}

			@Override
			public void cancel() {
				isRunning = false;
			}
		}, Types.ROW(Types.STRING, Types.LONG));

		return dataStream;

	}

	@Override
	public TypeInformation<Row> getReturnType() {
		TypeInformation<Row> typeInformation = new RowTypeInfo(fieldTypes, fieldNames);
		return typeInformation;
	}

	@Override
	public TableSchema getTableSchema() {
		return TableSchema.builder()
			.field("guid", Types.STRING)
			.field("eventTimestamp", Types.LONG)
			.build();
	}

	@Override
	public String explainSource() {
		return "";
	}
}
