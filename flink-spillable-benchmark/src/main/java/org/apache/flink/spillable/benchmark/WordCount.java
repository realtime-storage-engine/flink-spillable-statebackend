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

package org.apache.flink.spillable.benchmark;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.Collector;

import java.io.IOException;

import static org.apache.flink.spillable.benchmark.JobConfig.CHECKPOINT_INTERVAL;
import static org.apache.flink.spillable.benchmark.JobConfig.JOB_NAME;
import static org.apache.flink.spillable.benchmark.JobConfig.WORD_LENGTH;
import static org.apache.flink.spillable.benchmark.JobConfig.WORD_NUMBER;
import static org.apache.flink.spillable.benchmark.JobConfig.WORD_RATE;
import static org.apache.flink.spillable.benchmark.JobConfig.getConfiguration;

/**
 * A word count job.
 */
public class WordCount {

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		ParameterTool params = ParameterTool.fromArgs(args);
		Configuration configuration = getConfiguration(params);

		env.getConfig().setGlobalJobParameters(configuration);
		env.setParallelism(1);
		env.disableOperatorChaining();

		String jobName = configuration.getString(JOB_NAME);

		env.enableCheckpointing(configuration.getLong(CHECKPOINT_INTERVAL), CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().enableExternalizedCheckpoints(
				CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		// configure source
		int wordNumber = configuration.getInteger(WORD_NUMBER);
		int wordLength = configuration.getInteger(WORD_LENGTH);
		int wordRate = configuration.getInteger(WORD_RATE);

		DataStream<Tuple2<String, Long>> source = WordSource.getSource(env, wordRate, wordNumber, wordLength)
						.slotSharingGroup("src");

		DataStream<Tuple2<String, Integer>> mapper = source.keyBy(0)
				.flatMap(new Counter())
				.slotSharingGroup("map");

		mapper.addSink(new DiscardingSink<>())
				.slotSharingGroup("sink");

		env.execute(jobName);
	}

	/**
	 * Write and read mixed mapper.
	 */
	public static class Counter extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, Integer>> {

		private transient ValueState<Integer> wordCounter;

		public Counter() {
		}

		@Override
		public void flatMap(Tuple2<String, Long> in, Collector<Tuple2<String, Integer>> out) throws IOException {
			Integer currentValue = wordCounter.value();

			currentValue = currentValue == null ? 1 : currentValue + 1;
			wordCounter.update(currentValue);

			out.collect(Tuple2.of(in.f0, currentValue));
		}

		@Override
		public void open(Configuration config) {
			ValueStateDescriptor<Integer> descriptor =
					new ValueStateDescriptor<>(
							"wc",
							TypeInformation.of(new TypeHint<Integer>(){}));
			wordCounter = getRuntimeContext().getState(descriptor);
		}
	}
}
