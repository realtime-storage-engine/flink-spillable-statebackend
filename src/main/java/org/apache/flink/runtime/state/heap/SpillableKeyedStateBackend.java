/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StateSnapshotTransformers;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.space.SpaceAllocator;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.StateMigrationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A {@link AbstractKeyedStateBackend} that keeps state on the Java Heap first, and will spill
 * part of states to disk when GC pressure grows higher.
 *
 * @param <K> The key by which state is keyed.
 */
public class SpillableKeyedStateBackend<K> extends HeapKeyedStateBackend<K> {

	private static final Logger LOG = LoggerFactory.getLogger(SpillableKeyedStateBackend.class);

	private static final Map<Class<? extends StateDescriptor>, StateFactory> SPILLABLE_STATE_FACTORIES =
		Stream.of(
			Tuple2.of(ValueStateDescriptor.class, (StateFactory) SpillableValueState::create),
			Tuple2.of(ListStateDescriptor.class, (StateFactory) SpillableListState::create),
			Tuple2.of(MapStateDescriptor.class, (StateFactory) SpillableMapState::create),
			Tuple2.of(AggregatingStateDescriptor.class, (StateFactory) SpillableAggregatingState::create),
			Tuple2.of(ReducingStateDescriptor.class, (StateFactory) SpillableReducingState::create),
			Tuple2.of(FoldingStateDescriptor.class, (StateFactory) SpillableFoldingState::create)
		).collect(Collectors.toMap(t -> t.f0, t -> t.f1));

	/**
	 * Map of registered Key/Value states.
	 */
	private final Map<String, StateTable<K, ?, ?>> registeredKVStates;

	/**
	 * Map of registered priority queue set states.
	 */
	private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates;

	/**
	 * The configuration for local recovery.
	 */
	private final LocalRecoveryConfig localRecoveryConfig;

	/**
	 * The snapshot strategy for this backend. This determines, e.g., if snapshots are synchronous or asynchronous.
	 */
	private final HeapSnapshotStrategy<K> snapshotStrategy;

	/**
	 * Factory for state that is organized as priority queue.
	 */
	private final HeapPriorityQueueSetFactory priorityQueueSetFactory;

	private final SpaceAllocator spaceAllocator;
	private final File[] localPaths;
	private final SpillAndLoadManager spillAndLoadManager;
	private final CheckpointManager checkpointManager;

	public SpillableKeyedStateBackend(
		TaskKvStateRegistry kvStateRegistry,
		TypeSerializer<K> keySerializer,
		ClassLoader userCodeClassLoader,
		ExecutionConfig executionConfig,
		TtlTimeProvider ttlTimeProvider,
		CloseableRegistry cancelStreamRegistry,
		StreamCompressionDecorator keyGroupCompressionDecorator,
		Map<String, StateTable<K, ?, ?>> registeredKVStates,
		Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates,
		LocalRecoveryConfig localRecoveryConfig,
		HeapPriorityQueueSetFactory priorityQueueSetFactory,
		SpillableSnapshotStrategy<K> snapshotStrategy,
		InternalKeyContext<K> keyContext,
		SpaceAllocator spaceAllocator,
		SpillAndLoadManager spillAndLoadManager,
		CheckpointManager checkpointManager,
		File[] localPaths) {
		super(
			kvStateRegistry,
			keySerializer,
			userCodeClassLoader,
			executionConfig,
			ttlTimeProvider,
			cancelStreamRegistry,
			keyGroupCompressionDecorator,
			registeredKVStates,
			registeredPQStates,
			localRecoveryConfig,
			priorityQueueSetFactory,
			snapshotStrategy,
			keyContext);
		this.registeredKVStates = registeredKVStates;
		this.registeredPQStates = registeredPQStates;
		this.localRecoveryConfig = localRecoveryConfig;
		this.priorityQueueSetFactory = priorityQueueSetFactory;
		this.snapshotStrategy = snapshotStrategy;

		this.spaceAllocator = spaceAllocator;
		this.spillAndLoadManager = spillAndLoadManager;
		this.checkpointManager = checkpointManager;
		this.localPaths = localPaths;
		LOG.info("SpillableKeyedStateBackend is initialized.");
	}

	@Override
	@Nonnull
	public <N, SV, SEV, S extends State, IS extends S> IS createInternalState(
		@Nonnull TypeSerializer<N> namespaceSerializer,
		@Nonnull StateDescriptor<S, SV> stateDesc,
		@Nonnull StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> snapshotTransformFactory) throws Exception {
		StateFactory stateFactory = SPILLABLE_STATE_FACTORIES.get(stateDesc.getClass());
		if (stateFactory == null) {
			String message = String.format("State %s is not supported by %s",
				stateDesc.getClass(), this.getClass());
			throw new FlinkRuntimeException(message);
		}
		StateTable<K, N, SV> stateTable = tryRegisterStateTable(
			namespaceSerializer, stateDesc, getStateSnapshotTransformFactory(stateDesc, snapshotTransformFactory));
		return stateFactory.createState(stateDesc, stateTable, getKeySerializer());
	}

	@Override
	public void dispose() {
		super.dispose();

		for (StateTable stateTable : registeredKVStates.values()) {
			IOUtils.closeQuietly((SpillableStateTableImpl) stateTable);
		}

		// TODO how to safely close space allocator, for example in the case
		// some snapshots are still use the space
		IOUtils.closeQuietly(spaceAllocator);

		for (File dir : localPaths) {
			try {
				FileUtils.deleteDirectory(dir);
			} catch (IOException ex) {
				LOG.warn("Could not delete working directory: {}", dir, ex);
			}
		}
	}

	private <N, V> StateTable<K, N, V> tryRegisterStateTable(
		TypeSerializer<N> namespaceSerializer,
		StateDescriptor<?, V> stateDesc,
		@Nonnull StateSnapshotTransformer.StateSnapshotTransformFactory<V> snapshotTransformFactory) throws StateMigrationException {

		@SuppressWarnings("unchecked")
		StateTable<K, N, V> stateTable = (StateTable<K, N, V>) registeredKVStates.get(stateDesc.getName());

		TypeSerializer<V> newStateSerializer = stateDesc.getSerializer();

		if (stateTable != null) {
			RegisteredKeyValueStateBackendMetaInfo<N, V> restoredKvMetaInfo = stateTable.getMetaInfo();

			restoredKvMetaInfo.updateSnapshotTransformFactory(snapshotTransformFactory);

			TypeSerializerSchemaCompatibility<N> namespaceCompatibility =
				restoredKvMetaInfo.updateNamespaceSerializer(namespaceSerializer);
			if (namespaceCompatibility.isCompatibleAfterMigration() || namespaceCompatibility.isIncompatible()) {
				throw new StateMigrationException("For heap backends, the new namespace serializer must be compatible.");
			}

			restoredKvMetaInfo.checkStateMetaInfo(stateDesc);

			TypeSerializerSchemaCompatibility<V> stateCompatibility =
				restoredKvMetaInfo.updateStateSerializer(newStateSerializer);

			if (stateCompatibility.isIncompatible()) {
				throw new StateMigrationException("For heap backends, the new state serializer must not be incompatible.");
			}

			stateTable.setMetaInfo(restoredKvMetaInfo);
		} else {
			RegisteredKeyValueStateBackendMetaInfo<N, V> newMetaInfo = new RegisteredKeyValueStateBackendMetaInfo<>(
				stateDesc.getType(),
				stateDesc.getName(),
				namespaceSerializer,
				newStateSerializer,
				snapshotTransformFactory);

			stateTable = snapshotStrategy.newStateTable(keyContext, newMetaInfo, keySerializer);
			registeredKVStates.put(stateDesc.getName(), stateTable);
		}

		return stateTable;
	}

	@SuppressWarnings("unchecked")
	private <SV, SEV> StateSnapshotTransformer.StateSnapshotTransformFactory<SV> getStateSnapshotTransformFactory(
		StateDescriptor<?, SV> stateDesc,
		StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> snapshotTransformFactory) {
		if (stateDesc instanceof ListStateDescriptor) {
			return (StateSnapshotTransformer.StateSnapshotTransformFactory<SV>) new StateSnapshotTransformers.ListStateSnapshotTransformFactory<>(snapshotTransformFactory);
		} else if (stateDesc instanceof MapStateDescriptor) {
			return (StateSnapshotTransformer.StateSnapshotTransformFactory<SV>) new StateSnapshotTransformers.MapStateSnapshotTransformFactory<>(snapshotTransformFactory);
		} else {
			return (StateSnapshotTransformer.StateSnapshotTransformFactory<SV>) snapshotTransformFactory;
		}
	}

	private interface StateFactory {
		<K, N, SV, S extends State, IS extends S> IS createState(
			StateDescriptor<S, SV> stateDesc,
			StateTable<K, N, SV> stateTable,
			TypeSerializer<K> keySerializer) throws Exception;
	}

	@VisibleForTesting
	CheckpointManager getCheckpointManager() {
		return checkpointManager;
	}
}
