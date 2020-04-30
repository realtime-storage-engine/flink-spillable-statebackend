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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Estimates memory usage of {@link MapState}.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 */
public class MapStateMemoryEstimator<K, N> extends AbstractStateMemoryEstimator<K, N, Map>  {

	private static final long HASH_MAP_SHADOW_SIZE;
	private static final long ENTRY_SHADOW_SIZE;

	static {
		Map<Object, Object> testMap = new HashMap<>();
		testMap.put(1, 1);
		HASH_MAP_SHADOW_SIZE = RamUsageEstimator.shallowSizeOf(testMap);
		ENTRY_SHADOW_SIZE = RamUsageEstimator.shallowSizeOf(testMap.entrySet().iterator().next());
	}

	private boolean isMapKeyFix;
	private long mapKeySize;

	private boolean isMapValueFix;
	private long mapValueSize;

	public MapStateMemoryEstimator(
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		MapSerializer mapSerializer) {
		super(keySerializer, namespaceSerializer);
		this.isMapKeyFix = mapSerializer.getKeySerializer().getLength() != -1;
		this.mapKeySize = -1;
		this.isMapValueFix = mapSerializer.getValueSerializer().getLength() != -1;
		this.mapValueSize = -1;
	}

	@Override
	protected long getStateSize(Map state) {
		return getDataStructureSize(state) + getMapKeyValueSize(state);
	}

	/**
	 * Approximate size of {@link HashMap} excluding keys and values.
	 */
	private long getDataStructureSize(Map map) {
		assert map instanceof HashMap;
		int size = map.size();
		long arraySize = RamUsageEstimator.alignObjectSize(
			(long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER +
			(long) RamUsageEstimator.NUM_BYTES_OBJECT_REF * size);

		return HASH_MAP_SHADOW_SIZE + ENTRY_SHADOW_SIZE * size + arraySize;
	}

	private long getMapKeyValueSize(Map state) {
		if (state.size() == 0) {
			return 0;
		}

		long totalSize = 0;
		// if map key or value is not fixed, iterates all entries
		if (!isMapKeyFix || !isMapValueFix) {
			for (Object object : state.entrySet()) {
				Map.Entry entry = (Map.Entry) object;
				totalSize += getMapKeySize(entry.getKey()) + getMapValueSize(entry.getValue());
			}
		} else {
			Map.Entry entry = (Map.Entry) (state.entrySet().iterator().next());
			totalSize = state.size() * (getMapKeySize(entry.getKey()) + getMapValueSize(entry.getValue()));
		}

		return totalSize;
	}

	private long getMapKeySize(Object mapKey) {
		if (!isMapKeyFix || mapKeySize == -1) {
			this.mapKeySize = RamUsageEstimator.sizeOf(mapKey);
		}

		return mapKeySize;
	}

	private long getMapValueSize(Object mapValue) {
		if (!isMapValueFix || mapValueSize == -1) {
			this.mapValueSize = RamUsageEstimator.sizeOf(mapValue);
		}

		return mapValueSize;
	}
}
