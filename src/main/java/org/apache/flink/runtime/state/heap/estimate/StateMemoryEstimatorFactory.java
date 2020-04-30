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

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;

/**
 * Factory to create {@link StateMemoryEstimator}.
 */
public class StateMemoryEstimatorFactory {

	@SuppressWarnings("unchecked")
	public static <K, N, S> SampleStateMemoryEstimator<K, N, S> createSampleEstimator(
		TypeSerializer<K> keySerializer,
		RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
		int estimateSampleCount) {
		StateDescriptor.Type stateType = metaInfo.getStateType();
		StateMemoryEstimator<K, N, ?> stateMemoryEstimator;
		switch (stateType) {
			case VALUE:
			case FOLDING:
			case REDUCING:
			case AGGREGATING:
				stateMemoryEstimator = new ValueStateMemoryEstimator<>(
					keySerializer, metaInfo.getNamespaceSerializer(), metaInfo.getStateSerializer());
				break;
			case MAP:
				stateMemoryEstimator = new MapStateMemoryEstimator<>(
					keySerializer, metaInfo.getNamespaceSerializer(), (MapSerializer) metaInfo.getStateSerializer());
				break;
			case LIST:
				stateMemoryEstimator = new ListStateMemoryEstimator<>(
					keySerializer, metaInfo.getNamespaceSerializer(), (ListSerializer) metaInfo.getStateSerializer());
				break;
			default:
				throw new UnsupportedOperationException("Unknown state type " + stateType.name());
		}

		return new SampleStateMemoryEstimator<>((StateMemoryEstimator<K, N, S>) stateMemoryEstimator, estimateSampleCount);
	}
}
