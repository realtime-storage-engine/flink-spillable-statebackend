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

import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link CheckpointManager}.
 */
public class CheckpointManagerTest {

	@Test
	public void testRegisterCheckpoint() {
		CheckpointManagerImpl checkpointManager = new CheckpointManagerImpl();
		Map<Long, TestRunnableFuture> expectedFutures = new HashMap<>();
		for (long i = 0; i < 10; i++) {
			TestRunnableFuture future = new TestRunnableFuture();
			assertTrue(checkpointManager.registerCheckpoint(i, future));
			expectedFutures.put(i, future);
		}

		// register existed checkpoints
		for (long i = 0; i < 10; i++) {
			TestRunnableFuture future = new TestRunnableFuture();
			assertFalse(checkpointManager.registerCheckpoint(i, future));
		}

		verifyRunningCheckpoints(expectedFutures, checkpointManager);
	}

	@Test
	public void testUnRegisterCheckpoint() {
		CheckpointManagerImpl checkpointManager = new CheckpointManagerImpl();
		Map<Long, TestRunnableFuture> expectedFutures = new HashMap<>();
		for (long i = 0; i < 10; i++) {
			TestRunnableFuture future = new TestRunnableFuture();
			assertTrue(checkpointManager.registerCheckpoint(i, future));
			expectedFutures.put(i, future);
		}

		// unregister some checkpoints
		for (long i = 0; i < 10; i += 2) {
			assertTrue(checkpointManager.unRegisterCheckpoint(i));
			// verify that checkpoint is only unregistered but not cancelled and run
			assertFalse(expectedFutures.get(i).isCancelled());
			assertFalse(expectedFutures.get(i).isDone());
			expectedFutures.remove(i);
		}

		// unregister non-exist checkpoints
		for (long i = 0; i < 10; i += 2) {
			assertFalse(checkpointManager.unRegisterCheckpoint(i));
		}

		verifyRunningCheckpoints(expectedFutures, checkpointManager);
	}

	@Test
	public void testCancelCheckpoint() {
		CheckpointManagerImpl checkpointManager = new CheckpointManagerImpl();
		Map<Long, TestRunnableFuture> expectedFutures = new HashMap<>();
		for (long i = 0; i < 10; i++) {
			TestRunnableFuture future = new TestRunnableFuture();
			assertTrue(checkpointManager.registerCheckpoint(i, future));
			expectedFutures.put(i, future);
		}

		for (long i = 1; i < 10; i += 2) {
			assertTrue(checkpointManager.cancelCheckpoint(i));
			verifyCheckpointIsCancelled(i, checkpointManager);
		}

		// cancel non-exist checkpoints
		for (long i = 10; i < 20; i += 2) {
			assertFalse(checkpointManager.cancelCheckpoint(i));
		}

		// verify checkpoints are just cancelled but not unregistered
		verifyRunningCheckpoints(expectedFutures, checkpointManager);
	}

	@Test
	public void testCancelAllCheckpoints() {
		CheckpointManagerImpl checkpointManager = new CheckpointManagerImpl();
		Map<Long, TestRunnableFuture> expectedFutures = new HashMap<>();
		for (long i = 0; i < 10; i++) {
			TestRunnableFuture future = new TestRunnableFuture();
			assertTrue(checkpointManager.registerCheckpoint(i, future));
			expectedFutures.put(i, future);
		}

		checkpointManager.cancelAllCheckpoints();
		for (long i = 0; i < 10; i++) {
			verifyCheckpointIsCancelled(i, checkpointManager);
		}

		// verify checkpoints are just cancelled but not unregistered
		verifyRunningCheckpoints(expectedFutures, checkpointManager);
	}

	private void verifyCheckpointIsCancelled(long checkpointId, CheckpointManagerImpl checkpointManager) {
		TestRunnableFuture future = (TestRunnableFuture) checkpointManager.getRunningCheckpoints().get(checkpointId);
		assertNotNull(future);
		assertFalse(future.isDone());
		assertTrue(future.isCancelled());
		assertTrue(future.isMayInterruptIfRunning());
	}

	private void verifyRunningCheckpoints(
		Map<Long, TestRunnableFuture> expectedFutures,
		CheckpointManagerImpl checkpointManager) {
		Map<Long, RunnableFuture<SnapshotResult<KeyedStateHandle>>> actualFutures =
			checkpointManager.getRunningCheckpoints();
		assertEquals(expectedFutures.size(), actualFutures.size());
		for (Map.Entry<Long, TestRunnableFuture> entry : expectedFutures.entrySet()) {
			RunnableFuture<SnapshotResult<KeyedStateHandle>> future = actualFutures.get(entry.getKey());
			assertSame(entry.getValue(), future);
		}
	}

	static class TestRunnableFuture implements RunnableFuture<SnapshotResult<KeyedStateHandle>> {

		private boolean isDone;
		private boolean cancelled;
		private boolean mayInterruptIfRunning;

		public TestRunnableFuture() {
			this.cancelled = false;
			this.mayInterruptIfRunning = false;
		}

		@Override
		public void run() {
			this.isDone = true;
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			this.cancelled = true;
			this.mayInterruptIfRunning = mayInterruptIfRunning;
			return true;
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public boolean isDone() {
			return isDone;
		}

		@Override
		public SnapshotResult<KeyedStateHandle> get() {
			throw new UnsupportedOperationException();
		}

		@Override
		public SnapshotResult<KeyedStateHandle> get(long timeout, TimeUnit unit) {
			throw new UnsupportedOperationException();

		}

		public boolean isMayInterruptIfRunning() {
			return mayInterruptIfRunning;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			}

			if (obj == null || obj.getClass() != getClass()) {
				return false;
			}

			TestRunnableFuture other = (TestRunnableFuture) obj;
			return isDone == other.isDone &&
				cancelled == other.cancelled &&
				mayInterruptIfRunning == other.mayInterruptIfRunning;
		}
	}
}
