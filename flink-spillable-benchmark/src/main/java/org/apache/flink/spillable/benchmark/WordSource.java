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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

/**
 * Source to emit words at the given rate.
 */
public class WordSource  extends RichParallelSourceFunction<Tuple2<String, Long>> {

	private static final long serialVersionUID = 1L;

	private final int wordLen;

	private final int wordNum;

	private final long wordRate;

	private transient ThrottledIterator<Integer> throttledIterator;

	private transient char[] fatArray;

	private transient int emitNumber;

	private transient volatile boolean isRunning;

	public WordSource(int wordNum, long wordRate, int wordLen) {
		this.wordLen = wordLen;
		this.wordNum = wordNum;
		this.wordRate = wordRate;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		this.isRunning = true;
		this.emitNumber = 0;

		Iterator<Integer> numberSourceIterator = new NumberSourceIterator(wordNum, System.currentTimeMillis());
		this.throttledIterator = new ThrottledIterator<>(numberSourceIterator, wordRate);

		this.fatArray = new char[wordLen];
		Random random = new Random(0);
		for (int i = 0; i < fatArray.length; i++) {
			fatArray[i] = (char) random.nextInt();
		}
	}

	@Override
	public void run(SourceContext<Tuple2<String, Long>> sourceContext) throws Exception {
		while (isRunning) {
			Integer number;
			// first emit all keys
			if (emitNumber < wordNum) {
				number = emitNumber++;
			} else {
				number = throttledIterator.next();
			}
			sourceContext.collect(Tuple2.of(covertToString(number), System.currentTimeMillis()));
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
	}

	@Override
	public void close() {
		isRunning = false;
	}

	public static DataStreamSource<Tuple2<String, Long>> getSource(StreamExecutionEnvironment env, long wordRate, int wordNum, int wordLen) {
		return env.addSource(new WordSource(wordNum, wordRate, wordLen));
	}

	private String covertToString(int number) {
		String a = String.valueOf(number);
		StringBuilder builder = new StringBuilder(wordLen);
		builder.append(a);
		builder.append(fatArray, 0, wordLen - a.length());
		return builder.toString();
	}

	// ------------------------------------------------------------------------
	//  Number generator
	// ------------------------------------------------------------------------

	static class NumberSourceIterator implements Iterator<Integer>, Serializable {
		private final int largest;
		private final Random rnd;

		public NumberSourceIterator(int largest, long seed) {
			this.largest = largest;
			this.rnd = new Random(seed);
		}

		@Override
		public boolean hasNext() {
			return true;
		}

		@Override
		public Integer next() {
			Integer value = rnd.nextInt(largest + 1);
			return value;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}
