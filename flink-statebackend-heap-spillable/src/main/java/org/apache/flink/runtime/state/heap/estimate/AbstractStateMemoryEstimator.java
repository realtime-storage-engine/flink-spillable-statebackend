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

import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * Base class for state memory estimation.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
public abstract class AbstractStateMemoryEstimator<K, N, S> implements StateMemoryEstimator<K, N, S> {

	private boolean isKeyFix;
	private long keySize;

	private boolean isNamespaceFix;
	private long namespaceSize;

	private long stateSize;

	public AbstractStateMemoryEstimator(
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer) {
		this.isKeyFix = keySerializer.getLength() != -1;
		this.keySize = -1;
		this.isNamespaceFix = namespaceSerializer.getLength() != -1;
		this.namespaceSize = -1;
	}

	@Override
	public void updateEstimatedSize(K key, N namespace, S state) {
		if (keySize == -1 || !isKeyFix) {
			this.keySize = RamUsageEstimator.sizeOf(key);
		}

		if (namespaceSize == -1 || !isNamespaceFix) {
			this.namespaceSize = RamUsageEstimator.sizeOf(key);
		}

		this.stateSize = getStateSize(state);
	}

	@Override
	public long getEstimatedSize() {
		return keySize + namespaceSize + stateSize;
	}

	protected abstract long getStateSize(S state);
}
