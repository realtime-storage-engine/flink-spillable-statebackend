/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.flink.runtime.state.heap.estimate;

import org.apache.flink.util.Preconditions;

/**
 * Sample states to estimate memory usage.
 */
public class SampleStateMemoryEstimator<K, N, S> implements StateMemoryEstimator<K, N, S> {

	private final StateMemoryEstimator<K, N, S> stateMemoryEstimator;
	private final int sampleCount;

	private long count;
	private long totalSize;
	private long totalNum;

	public SampleStateMemoryEstimator(
		StateMemoryEstimator<K, N, S> stateMemoryEstimator,
		int sampleCount) {
		this.stateMemoryEstimator = Preconditions.checkNotNull(stateMemoryEstimator);
		Preconditions.checkArgument(sampleCount > 0, "Sample count should be positive");
		this.sampleCount = sampleCount;
		this.count = 0;
		this.totalSize = 0;
		this.totalNum = 0;
	}

	@Override
	public void updateEstimatedSize(K key, N namespace, S state) {
		if (count++ % sampleCount == 0) {
			forceUpdateEstimatedSize(key, namespace, state);
		}
	}

	@Override
	public long getEstimatedSize() {
		return totalNum > 0 ? totalSize / totalNum : -1;
	}

	public void forceUpdateEstimatedSize(K key, N namespace, S state) {
		stateMemoryEstimator.updateEstimatedSize(key, namespace, state);
		totalSize += stateMemoryEstimator.getEstimatedSize();
		totalNum++;
	}
}
