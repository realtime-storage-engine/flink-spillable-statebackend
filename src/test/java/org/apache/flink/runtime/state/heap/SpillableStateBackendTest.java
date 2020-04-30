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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateBackendTestBase;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.concurrent.RunnableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link SpillableStateBackend}.
 */
public class SpillableStateBackendTest extends StateBackendTestBase<SpillableStateBackend> {

	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	@Override
	protected SpillableStateBackend getStateBackend() throws IOException {
		SpillableStateBackend backend = new SpillableStateBackend(new MemoryStateBackend(true));

		String dbPath = tempFolder.newFolder().getAbsolutePath();
		backend.setDbStoragePaths(dbPath);

		return backend;
	}

	@Override
	protected boolean isSerializerPresenceRequiredOnRestore() {
		return true;
	}

	// disable these because the verification does not work for this state backend
	@Override
	@Test
	public void testValueStateRestoreWithWrongSerializers() {}

	@Override
	@Test
	public void testListStateRestoreWithWrongSerializers() {}

	@Override
	@Test
	public void testReducingStateRestoreWithWrongSerializers() {}

	@Override
	@Test
	public void testMapStateRestoreWithWrongSerializers() {}

	@Ignore
	@Test
	public void testConcurrentMapIfQueryable() throws Exception {
		super.testConcurrentMapIfQueryable();
	}

	@Test
	public void testCheckpointManagerWithSnapshotCancellation() throws Exception {
		testCheckpointManager(true);
	}

	@Test
	public void testCheckpointManagerWithNormalSnapshot() throws Exception {
		testCheckpointManager(false);
	}

	private void testCheckpointManager(boolean cancel) throws Exception {
		CheckpointStreamFactory streamFactory = createStreamFactory();
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

		SpillableKeyedStateBackend<Integer> backend =
			(SpillableKeyedStateBackend<Integer>) createKeyedBackend(IntSerializer.INSTANCE);
		CheckpointManagerImpl checkpointManager = (CheckpointManagerImpl) backend.getCheckpointManager();
		ValueState<String> state = backend.getPartitionedState(
			VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, new ValueStateDescriptor<>("id", String.class));

		// some modifications to the state
		backend.setCurrentKey(1);
		state.update("1");
		backend.setCurrentKey(2);
		state.update("2");

		RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
			backend.snapshot(0L, 0L, streamFactory, CheckpointOptions.forCheckpointWithDefaultLocation());

		assertEquals(1, checkpointManager.getRunningCheckpoints().size());
		RunnableFuture<SnapshotResult<KeyedStateHandle>> future = checkpointManager.getRunningCheckpoints().get(0L);
		assertSame(snapshot, future);

		if (cancel) {
			snapshot.cancel(true);
		} else {
			runSnapshot(snapshot, sharedStateRegistry);
		}

		assertTrue(checkpointManager.getRunningCheckpoints().isEmpty());

		backend.dispose();
	}
}
