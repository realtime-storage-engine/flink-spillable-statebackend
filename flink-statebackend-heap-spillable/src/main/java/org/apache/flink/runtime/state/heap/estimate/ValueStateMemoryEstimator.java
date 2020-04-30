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

package org.apache.flink.runtime.state.heap.estimate;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * Estimates memory usage of {@link ValueState}.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <V> type of state
 */
public class ValueStateMemoryEstimator<K, N, V> extends AbstractStateMemoryEstimator<K, N, V> {

	private boolean isStateFix;
	private long stateSize;

	public ValueStateMemoryEstimator(
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<V> valueSerializer) {
		super(keySerializer, namespaceSerializer);
		this.isStateFix = valueSerializer.getLength() != -1;
		this.stateSize = -1;
	}

	@Override
	protected long getStateSize(V state) {
		if (stateSize == -1 || !isStateFix) {
			this.stateSize = RamUsageEstimator.sizeOf(state);
		}

		return this.stateSize;
	}
}
