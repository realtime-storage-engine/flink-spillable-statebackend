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
import org.apache.flink.api.common.typeutils.base.ListSerializer;

import java.util.ArrayList;
import java.util.List;

/**
 * Estimates memory usage of {@link org.apache.flink.api.common.state.ListState}.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 */
public class ListStateMemoryEstimator<K, N> extends AbstractStateMemoryEstimator<K, N, List> {

	private static final long LIST_SHADOW_SIZE;

	static {
		List<Object> list = new ArrayList<>();
		LIST_SHADOW_SIZE = RamUsageEstimator.shallowSizeOf(list);
	}

	private boolean isElementFix;
	private long elementSize;

	public ListStateMemoryEstimator(
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		ListSerializer listSerializer) {
		super(keySerializer, namespaceSerializer);
		this.isElementFix = listSerializer.getElementSerializer().getLength() != -1;
		this.elementSize = -1;
	}

	@Override
	protected long getStateSize(List state) {
		return getDataStructureSize(state) + getListSize(state);
	}

	private long getDataStructureSize(List list) {
		assert list instanceof ArrayList;
		int size = list.size();
		long arraySize = RamUsageEstimator.alignObjectSize(
			(long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER +
			(long) RamUsageEstimator.NUM_BYTES_OBJECT_REF * size);

		return LIST_SHADOW_SIZE + arraySize;
	}

	private long getListSize(List list) {
		if (list.isEmpty()) {
			return 0;
		}

		long totalSize = 0;
		if (isElementFix) {
			totalSize += getElementSize(list.get(0)) * list.size();
		} else {
			for (Object element : list) {
				totalSize += getElementSize(element);
			}
		}

		return totalSize;
	}

	private long getElementSize(Object element) {
		if (!isElementFix || elementSize == -1) {
			this.elementSize = RamUsageEstimator.sizeOf(element);
		}

		return elementSize;
	}
}
