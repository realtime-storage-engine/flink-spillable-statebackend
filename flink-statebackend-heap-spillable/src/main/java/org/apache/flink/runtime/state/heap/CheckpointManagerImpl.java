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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RunnableFuture;

/**
 * Implementation of {@link CheckpointManager}.
 */
public class CheckpointManagerImpl implements CheckpointManager {

	private static final Logger LOG = LoggerFactory.getLogger(CheckpointManagerImpl.class);

	private Map<Long, RunnableFuture<SnapshotResult<KeyedStateHandle>>> runningCheckpoints;

	public CheckpointManagerImpl() {
		this.runningCheckpoints = new ConcurrentHashMap<>();
	}

	@Override
	public boolean registerCheckpoint(
		long checkpointId,
		RunnableFuture<SnapshotResult<KeyedStateHandle>> runnableFuture) {
		if (runningCheckpoints.containsKey(checkpointId)) {
			LOG.warn("Checkpoint {} has been registered.", checkpointId);
			return false;
		} else {
			runningCheckpoints.put(checkpointId, runnableFuture);
			LOG.debug("Register checkpoint {}.", checkpointId);
			return true;
		}
	}

	@Override
	public boolean unRegisterCheckpoint(long checkpointId) {
		if (runningCheckpoints.remove(checkpointId) != null) {
			LOG.debug("Unregister checkpoint {}.", checkpointId);
			return true;
		} else {
			LOG.warn("Try to unregister a non-exist checkpoint {}.", checkpointId);
			return false;
		}
	}

	@Override
	public boolean cancelCheckpoint(long checkpointId) {
		RunnableFuture<SnapshotResult<KeyedStateHandle>> runnableFuture =
			runningCheckpoints.get(checkpointId);
		if (runnableFuture != null) {
			runnableFuture.cancel(true);
			LOG.info("Cancel checkpoint {}.", checkpointId);
			return true;
		} else {
			LOG.warn("Try to cancel a non-exist checkpoint {}.", checkpointId);
			return false;
		}
	}

	@Override
	public void cancelAllCheckpoints() {
		for (Map.Entry<Long, RunnableFuture<SnapshotResult<KeyedStateHandle>>> entry : runningCheckpoints.entrySet()) {
			entry.getValue().cancel(true);
			LOG.info("Cancel checkpoint {}.", entry.getKey());
		}
	}

	@VisibleForTesting
	Map<Long, RunnableFuture<SnapshotResult<KeyedStateHandle>>> getRunningCheckpoints() {
		return runningCheckpoints;
	}
}
