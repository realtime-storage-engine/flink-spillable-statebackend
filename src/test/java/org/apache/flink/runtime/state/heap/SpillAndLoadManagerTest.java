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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.util.Preconditions;

import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link SpillAndLoadManagerImpl}.
 */
public class SpillAndLoadManagerTest {

	private static final long DEFAULT_MAX_MEMORY = 10240;
	private static final long DEFAULT_GC_TIME = 30000;
	private static final float DEFAULT_SPILL_SIZE_RATIO = 0.3f;
	private static final float DEFAULT_LOAD_START_RATIO = 0.2f;
	private static final long DEFAULT_LOAD_START_SIZE = (long) (DEFAULT_MAX_MEMORY * DEFAULT_LOAD_START_RATIO);
	private static final float DEFAULT_LOAD_END_RATIO = 0.4f;
	private static final long DEFAULT_LOAD_END_SIZE = (long) (DEFAULT_MAX_MEMORY * DEFAULT_LOAD_END_RATIO);
	private static final long DEFAULT_TRIGGER_INTERVAL = 500000;
	private static final long DEFAULT_RESOURCE_CHECK_INTERVAL = 5000;

	private TestHeapStatusMonitor statusMonitor;
	private CheckpointManagerImpl checkpointManager;

	@Before
	public void setUp() {
		this.statusMonitor = new TestHeapStatusMonitor(DEFAULT_MAX_MEMORY);
		this.checkpointManager = new CheckpointManagerImpl();
		checkpointManager.registerCheckpoint(1L, new CheckpointManagerTest.TestRunnableFuture());
	}

	@Test
	public void testConstruct() {
		Configuration conf = buildDefaultConf();
		SpillAndLoadManagerImpl manager = new SpillAndLoadManagerImpl(
			new TestStateTableContainer(new HashMap<>()), statusMonitor, checkpointManager, conf);

		assertEquals(DEFAULT_MAX_MEMORY, manager.getMaxMemory());
		assertEquals(DEFAULT_GC_TIME, manager.getGcTimeThreshold());
		assertEquals(DEFAULT_SPILL_SIZE_RATIO, manager.getSpillSizeRatio(), 1e-9f);
		assertEquals(DEFAULT_LOAD_START_SIZE, manager.getLoadStartSize());
		assertEquals(DEFAULT_LOAD_END_SIZE, manager.getLoadEndSize());
		assertEquals(DEFAULT_TRIGGER_INTERVAL, manager.getTriggerInterval());
		assertEquals(DEFAULT_RESOURCE_CHECK_INTERVAL, manager.getResourceCheckInterval());
	}

	@Test
	public void testDecideNoAction() {
		HeapStatusMonitor.MonitorResult monitorResult = new HeapStatusMonitor.MonitorResult(
			System.currentTimeMillis(), 1, DEFAULT_MAX_MEMORY,
			DEFAULT_LOAD_START_SIZE + 10,
			DEFAULT_GC_TIME - 10);
		testDecideActionBase(monitorResult, SpillAndLoadManagerImpl.ActionResult.ofNone());
	}

	@Test
	public void testDecideSpill() {
		HeapStatusMonitor.MonitorResult monitorResult = new HeapStatusMonitor.MonitorResult(
			System.currentTimeMillis(), 1, DEFAULT_MAX_MEMORY,
				(long) (DEFAULT_MAX_MEMORY * 0.9f),
			DEFAULT_GC_TIME + 10);
		testDecideActionBase(monitorResult, SpillAndLoadManagerImpl.ActionResult.ofSpill(DEFAULT_SPILL_SIZE_RATIO));
	}

	@Test
	public void testDecideLoad() {
		long usedMemory = DEFAULT_LOAD_START_SIZE - 100;
		float ratio = (float) (DEFAULT_LOAD_END_SIZE - usedMemory) / usedMemory;
		HeapStatusMonitor.MonitorResult monitorResult = new HeapStatusMonitor.MonitorResult(
			System.currentTimeMillis(), 1, DEFAULT_MAX_MEMORY,
			usedMemory,
			DEFAULT_GC_TIME - 10);
		testDecideActionBase(monitorResult, SpillAndLoadManagerImpl.ActionResult.ofLoad(ratio));
	}

	private void testDecideActionBase(
		HeapStatusMonitor.MonitorResult monitorResult, SpillAndLoadManagerImpl.ActionResult expectedAction) {
		Configuration conf = buildDefaultConf();
		SpillAndLoadManagerImpl manager = new SpillAndLoadManagerImpl(
			new TestStateTableContainer(new HashMap<>()), statusMonitor, checkpointManager, conf);

		SpillAndLoadManagerImpl.ActionResult actionResult = manager.decideAction(monitorResult);
		assertEquals(expectedAction.action, actionResult.action);
		if (expectedAction.action != SpillAndLoadManagerImpl.Action.NONE) {
			assertEquals(expectedAction.spillOrLoadRatio, actionResult.spillOrLoadRatio, 1e-6);
		}
	}

	@Test
	public void testSpillWithCancelCheckpoint() {
		testSpillBase(true);

		// verify checkpoint is cancelled
		assertEquals(1, checkpointManager.getRunningCheckpoints().size());
		CheckpointManagerTest.TestRunnableFuture future =
			(CheckpointManagerTest.TestRunnableFuture) checkpointManager.getRunningCheckpoints().get(1L);
		assertNotNull(future);
		assertTrue(future.isCancelled());
		assertTrue(future.isMayInterruptIfRunning());
	}

	@Test
	public void testSpillWithoutCancelCheckpoint() {
		testSpillBase(false);

		// verify checkpoint is not cancelled
		assertEquals(1, checkpointManager.getRunningCheckpoints().size());
		CheckpointManagerTest.TestRunnableFuture future =
			(CheckpointManagerTest.TestRunnableFuture) checkpointManager.getRunningCheckpoints().get(1L);
		assertNotNull(future);
		assertFalse(future.isCancelled());
	}

	private void testSpillBase(boolean cancelCheckpoint) {
		Map<String, TestSpillableStateTable> stateTableMap = new HashMap<>();
		Map<String, List<Integer>> expectedResult = new HashMap<>();

		// empty table
		TestSpillableStateTable table1 = new TestSpillableStateTable(
			new boolean[]{true, true, true},
			new int[]{0, 0, 0},
			new long[]{0, 0, 0},
			5);
		stateTableMap.put("table1", table1);

		// all data is on disk
		TestSpillableStateTable table2 = new TestSpillableStateTable(
			new boolean[]{false, false},
			new int[]{50, 100},
			new long[]{100, 50},
			5);
		stateTableMap.put("table2", table2);

		// all data is on heap
		TestSpillableStateTable table3 = new TestSpillableStateTable(
			new boolean[]{true, true, true},
			new int[]{10, 12, 14},
			new long[]{100, 50, 0},
			5);
		stateTableMap.put("table3", table3);

		// data is both on heap and disk
		TestSpillableStateTable table4 = new TestSpillableStateTable(
			new boolean[]{true, false, true, false},
			new int[]{13, 5, 11, 1922},
			new long[]{20, 5, 75, 100},
			5);
		stateTableMap.put("table4", table4);

		expectedResult.put("table1", Collections.emptyList());
		expectedResult.put("table2", Collections.emptyList());
		expectedResult.put("table3", Arrays.asList(2, 1));
		expectedResult.put("table4", Collections.singletonList(0));

		HeapStatusMonitor statusMonitor = new TestHeapStatusMonitor(DEFAULT_MAX_MEMORY);
		Configuration conf = buildDefaultConf();
		conf.setBoolean(SpillableOptions.CANCEL_CHECKPOINT, cancelCheckpoint);
		SpillAndLoadManagerImpl manager = new SpillAndLoadManagerImpl(
			new TestStateTableContainer(stateTableMap), statusMonitor, checkpointManager, conf);

		SpillAndLoadManagerImpl.ActionResult actionResult = SpillAndLoadManagerImpl.ActionResult.ofSpill(0.5f);

		manager.doSpill(actionResult);

		for (Map.Entry<String, List<Integer>> entry : expectedResult.entrySet()) {
			String stateTableName = entry.getKey();
			TestSpillableStateTable stateTable = stateTableMap.get(entry.getKey());
			List<Integer> expectedGroups = entry.getValue();
			List<Integer> actualSpill = stateTable.getSpillGroups();
			List<Integer> actualLoad = stateTable.getLoadGroups();
			assertEquals(stateTableName, expectedGroups, actualSpill);
			assertTrue(stateTableName, actualLoad.isEmpty());
		}
	}

	@Test
	public void testLoad() {
		Map<String, TestSpillableStateTable> stateTableMap = new HashMap<>();
		Map<String, List<Integer>> expectedResult = new HashMap<>();

		// empty table
		TestSpillableStateTable table1 = new TestSpillableStateTable(
			new boolean[]{false, false, false},
			new int[]{0, 0, 0},
			new long[]{0, 0, 0},
			5);
		stateTableMap.put("table1", table1);

		// all data is on heap
		TestSpillableStateTable table2 = new TestSpillableStateTable(
			new boolean[]{true, true},
			new int[]{50, 100},
			new long[]{100, 50},
			5);
		stateTableMap.put("table2", table2);

		// all data is on disk
		TestSpillableStateTable table3 = new TestSpillableStateTable(
			new boolean[]{false, false, false, false},
			new int[]{10, 12, 14, 15},
			new long[]{100, 50, 10, 0},
			5);
		stateTableMap.put("table3", table3);

		// data is both on heap and disk
		TestSpillableStateTable table4 = new TestSpillableStateTable(
			new boolean[]{false, true, false, true},
			new int[]{13, 5, 11, 1922},
			new long[]{20, 5, 75, 100},
			5);
		stateTableMap.put("table4", table4);

		expectedResult.put("table1", Collections.emptyList());
		expectedResult.put("table2", Collections.emptyList());
		expectedResult.put("table3", Arrays.asList(0, 1));
		expectedResult.put("table4", Collections.singletonList(2));

		testLoadBase(stateTableMap, SpillAndLoadManagerImpl.ActionResult.ofLoad(0.5f), expectedResult);
	}

	private void testLoadBase(
		Map<String, TestSpillableStateTable> stateTableMap,
		SpillAndLoadManagerImpl.ActionResult actionResult,
		Map<String, List<Integer>> expectedResult
		) {
		HeapStatusMonitor statusMonitor = new TestHeapStatusMonitor(DEFAULT_MAX_MEMORY);
		Configuration conf = buildDefaultConf();
		SpillAndLoadManagerImpl manager = new SpillAndLoadManagerImpl(
			new TestStateTableContainer(stateTableMap), statusMonitor, checkpointManager, conf);

		manager.doLoad(actionResult);

		for (Map.Entry<String, List<Integer>> entry : expectedResult.entrySet()) {
			String stateTableName = entry.getKey();
			TestSpillableStateTable stateTable = stateTableMap.get(entry.getKey());
			List<Integer> expectedGroups = entry.getValue();
			List<Integer> actualSpill = stateTable.getSpillGroups();
			List<Integer> actualLoad = stateTable.getLoadGroups();
			assertEquals(stateTableName, expectedGroups, actualLoad);
			assertTrue(stateTableName, actualSpill.isEmpty());
		}
	}

	private Configuration buildDefaultConf() {
		Configuration conf = new Configuration();
		conf.set(SpillableOptions.GC_TIME_THRESHOLD, Duration.ofMillis(DEFAULT_GC_TIME));
		conf.setFloat(SpillableOptions.SPILL_SIZE_RATIO, DEFAULT_SPILL_SIZE_RATIO);
		conf.setFloat(SpillableOptions.LOAD_START_RATIO, DEFAULT_LOAD_START_RATIO);
		conf.setFloat(SpillableOptions.LOAD_END_RATIO, DEFAULT_LOAD_END_RATIO);
		conf.set(SpillableOptions.TRIGGER_INTERVAL, Duration.ofMillis(DEFAULT_TRIGGER_INTERVAL));
		conf.set(SpillableOptions.RESOURCE_CHECK_INTERVAL, Duration.ofMillis(DEFAULT_RESOURCE_CHECK_INTERVAL));

		return conf;
	}

	/**
	 * A {@link SpillableStateTable} used for tests.
	 */
	static class TestSpillableStateTable extends SpillableStateTable<Integer, Integer, Integer> {

		private final int numGroups;
		private final boolean[] isOnHeap;
		private final int[] numStates;
		private final long[] numRequests;
		private final long estimatedSize;
		private List<Integer> spillGroups;
		private List<Integer> loadGroups;

		public TestSpillableStateTable(
			boolean[] isOnHeap, int[] numStates, long[] numRequests, long estimatedSize) {
			super(new InternalKeyContextImpl<>(new KeyGroupRange(0, isOnHeap.length - 1), isOnHeap.length),
				new RegisteredKeyValueStateBackendMetaInfo<>(
					StateDescriptor.Type.VALUE, "test", IntSerializer.INSTANCE, IntSerializer.INSTANCE),
				IntSerializer.INSTANCE);

			this.numGroups = isOnHeap.length;
			this.isOnHeap = isOnHeap;
			this.numStates = numStates;
			this.numRequests = numRequests;
			Preconditions.checkState(numGroups == numStates.length);
			Preconditions.checkState(numGroups == numRequests.length);
			this.estimatedSize = estimatedSize;
			this.spillGroups = new ArrayList<>();
			this.loadGroups = new ArrayList<>();
		}

		@Override
		protected StateMap<Integer, Integer, Integer> createStateMap() {
			return mock(StateMap.class);
		}

		@Nonnull
		@Override
		public StateSnapshot stateSnapshot() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void updateStateEstimate(Integer namespace, Integer state) {
			throw new UnsupportedOperationException();
		}

		@Override
		public long getStateEstimatedSize(boolean force) {
			return estimatedSize;
		}

		@Override
		public void spillState(int keyGroupIndex) {
			Preconditions.checkState(!spillGroups.contains(keyGroupIndex));
			spillGroups.add(keyGroupIndex);
		}

		@Override
		public void loadState(int keyGroupIndex) {
			Preconditions.checkState(!loadGroups.contains(keyGroupIndex));
			loadGroups.add(keyGroupIndex);
		}

		@Override
		public Iterator<StateMapMeta> stateMapIterator() {
			return new Iterator<StateMapMeta>() {
				int next = 0;

				@Override
				public boolean hasNext() {
					return next < numGroups;
				}

				@Override
				public StateMapMeta next() {
					StateMapMeta stateMapMeta = new StateMapMeta(
						TestSpillableStateTable.this, next,
						isOnHeap[next], numStates[next], numRequests[next]);
					next++;
					return stateMapMeta;
				}
			};
		}

		@Override
		public void close() throws IOException {
		}

		public List<Integer> getSpillGroups() {
			return spillGroups;
		}

		public List<Integer> getLoadGroups() {
			return loadGroups;
		}
	}

	static class TestStateTableContainer implements SpillAndLoadManagerImpl.StateTableContainer {

		private final Map<String, TestSpillableStateTable> registeredKVStates;

		TestStateTableContainer(Map<String, TestSpillableStateTable> registeredKVStates) {
			this.registeredKVStates = registeredKVStates;
		}

		@Override
		public Iterator<Tuple2<String, SpillableStateTable>> iterator() {
			return new Iterator<Tuple2<String, SpillableStateTable>>() {
				private final Iterator<Map.Entry<String, TestSpillableStateTable>> iter =
					registeredKVStates.entrySet().iterator();

				@Override
				public boolean hasNext() {
					return iter.hasNext();
				}

				@Override
				public Tuple2<String, SpillableStateTable> next() {
					Map.Entry<String, TestSpillableStateTable> entry = iter.next();
					return Tuple2.of(entry.getKey(), entry.getValue());
				}
			};
		}
	}
}
