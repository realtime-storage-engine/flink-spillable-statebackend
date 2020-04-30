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

import java.util.concurrent.RunnableFuture;

/**
 * Manages running checkpoints.
 */
public interface CheckpointManager {

	/**
	 * Register the checkpoint to run.
	 *
	 * @param checkpointId id of checkpoint to register
	 * @param runnableFuture future to run
	 *
	 * @return true if register successfully
	 */
	boolean registerCheckpoint(long checkpointId, RunnableFuture<SnapshotResult<KeyedStateHandle>> runnableFuture);

	/**
	 * Unregister a checkpoint.
	 *
	 * @param checkpointId id of checkpoint to unregister
	 * @return true if unregister successfully
	 */
	boolean unRegisterCheckpoint(long checkpointId);

	/**
	 * Cancel a checkpoint.
	 *
	 * @param checkpointId id of checkpoint to cancel
	 * @return true if cancel successfully
	 */
	boolean cancelCheckpoint(long checkpointId);

	/**
	 * Cancel all running checkpoints.
	 */
	void cancelAllCheckpoints();
}
