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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.DefaultOperatorStateBackendBuilder;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * This state backend holds the working state in the memory (JVM heap) first, and will spill part of
 * states to disk when GC pressure grows higher. The state backend checkpoints state as files to a
 * file system (hence the backend's name).
 */
public class SpillableStateBackend extends AbstractStateBackend implements ConfigurableStateBackend {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(SpillableStateBackend.class);

	/** The state backend that we use for creating checkpoint streams. */
	private final StateBackend checkpointStreamBackend;

	private Configuration configuration = new Configuration();

	/** Whether we already lazily initialized our local storage directories. */
	private transient boolean isInitialized;

	/** Base paths for directory, as configured.
	 * Null if not yet set, in which case the configuration values will be used.
	 * The configuration defaults to the TaskManager's temp directories. */
	@Nullable
	private File[] localDirectories;

	/** Base paths for mmap directory, as initialized. */
	private transient File[] initializedBasePaths;

	public SpillableStateBackend(@Nullable String checkpointDataUri) throws IOException {
		this(checkpointDataUri == null ? new MemoryStateBackend() : new FsStateBackend(checkpointDataUri));
	}

	public SpillableStateBackend(StateBackend checkpointStreamBackend) {
		this.checkpointStreamBackend = Preconditions.checkNotNull(checkpointStreamBackend);
	}

	private SpillableStateBackend(SpillableStateBackend original, ReadableConfig config, ClassLoader classLoader) {
		// reconfigure the state backend backing the streams
		final StateBackend originalStreamBackend = original.checkpointStreamBackend;
		this.checkpointStreamBackend = originalStreamBackend instanceof ConfigurableStateBackend
			? ((ConfigurableStateBackend) originalStreamBackend).configure(config, classLoader)
			: originalStreamBackend;

		this.configuration = new Configuration();
		this.configuration.addAll(SpillableOptions.filter(config));
		this.configuration.addAll(original.configuration);
	}

	@Override
	public SpillableStateBackend configure(ReadableConfig config, ClassLoader classLoader) {
		return new SpillableStateBackend(this, config, classLoader);
	}

	public StateBackend getCheckpointBackend() {
		return checkpointStreamBackend;
	}

	@Override
	public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
		Environment env,
		JobID jobID,
		String operatorIdentifier,
		TypeSerializer<K> keySerializer,
		int numberOfKeyGroups,
		KeyGroupRange keyGroupRange,
		TaskKvStateRegistry kvStateRegistry,
		TtlTimeProvider ttlTimeProvider,
		MetricGroup metricGroup,
		@Nonnull Collection<KeyedStateHandle> stateHandles,
		CloseableRegistry cancelStreamRegistry) throws IOException {

		initHeapStatusMonitor();
		lazyInitializeForJob(env, operatorIdentifier);

		TaskStateManager taskStateManager = env.getTaskStateManager();
		LocalRecoveryConfig localRecoveryConfig = taskStateManager.createLocalRecoveryConfig();
		HeapPriorityQueueSetFactory priorityQueueSetFactory =
			new HeapPriorityQueueSetFactory(keyGroupRange, numberOfKeyGroups, 128);

		return new SpillableKeyedStateBackendBuilder<>(
			kvStateRegistry,
			keySerializer,
			env.getUserCodeClassLoader().asClassLoader(),
			numberOfKeyGroups,
			keyGroupRange,
			env.getExecutionConfig(),
			ttlTimeProvider,
			stateHandles,
			AbstractStateBackend.getCompressionDecorator(env.getExecutionConfig()),
			localRecoveryConfig,
			priorityQueueSetFactory,
			cancelStreamRegistry,
			configuration,
			initializedBasePaths).build();
	}

	@Override
	public OperatorStateBackend createOperatorStateBackend(
		Environment env,
		String operatorIdentifier,
		@Nonnull Collection<OperatorStateHandle> stateHandles,
		CloseableRegistry cancelStreamRegistry) throws Exception {
		// TODO whether to support sync snapshot
		final boolean asyncSnapshots = true;
		return new DefaultOperatorStateBackendBuilder(
			env.getUserCodeClassLoader().asClassLoader(),
			env.getExecutionConfig(),
			asyncSnapshots,
			stateHandles,
			cancelStreamRegistry).build();
	}

	@Override
	public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer) throws IOException {
		return checkpointStreamBackend.resolveCheckpoint(externalPointer);
	}

	@Override
	public CheckpointStorageAccess createCheckpointStorage(JobID jobId) throws IOException {
		return checkpointStreamBackend.createCheckpointStorage(jobId);
	}

	/**
	 * Sets the directories which store mmap files. These directories do not need to be
	 * persistent, they can be ephemeral, meaning that they are lost on a machine failure,
	 * because state is persisted in checkpoints.
	 *
	 * <p>If nothing is configured, these directories default to the TaskManager's local
	 * temporary file directories.
	 *
	 * <p>Each keyed state backend will store their files on different paths.
	 *
	 * <p>Passing {@code null} to this function restores the default behavior, where the configured
	 * temp directories will be used.
	 *
	 * @param paths The paths across which store mmap files.
	 */
	public void setDbStoragePaths(String... paths) {
		if (paths == null) {
			localDirectories = null;
		}
		else if (paths.length == 0) {
			throw new IllegalArgumentException("empty paths");
		}
		else {
			File[] pp = new File[paths.length];

			for (int i = 0; i < paths.length; i++) {
				final String rawPath = paths[i];
				final String path;

				if (rawPath == null) {
					throw new IllegalArgumentException("null path");
				}
				else {
					// we need this for backwards compatibility, to allow URIs like 'file:///'...
					URI uri = null;
					try {
						uri = new Path(rawPath).toUri();
					}
					catch (Exception e) {
						// cannot parse as a path
					}

					if (uri != null && uri.getScheme() != null) {
						if ("file".equalsIgnoreCase(uri.getScheme())) {
							path = uri.getPath();
						}
						else {
							throw new IllegalArgumentException("Path " + rawPath + " has a non-local scheme");
						}
					}
					else {
						path = rawPath;
					}
				}

				pp[i] = new File(path);
				if (!pp[i].isAbsolute()) {
					throw new IllegalArgumentException("Relative paths are not supported");
				}
			}

			localDirectories = pp;
		}
	}

	/**
	 * Gets the configured local storage paths, or null, if none were configured.
	 *
	 * <p>If nothing is configured, these directories default to the TaskManager's local
	 * temporary file directories.
	 */
	public String[] getDbStoragePaths() {
		if (localDirectories == null) {
			return null;
		} else {
			String[] paths = new String[localDirectories.length];
			for (int i = 0; i < paths.length; i++) {
				paths[i] = localDirectories[i].toString();
			}
			return paths;
		}
	}

	private void lazyInitializeForJob(
		Environment env,
		@SuppressWarnings("unused") String operatorIdentifier) throws IOException {

		if (isInitialized) {
			return;
		}

		// initialize the paths where the local RocksDB files should be stored
		if (localDirectories == null) {
			// initialize from the temp directories
			initializedBasePaths = env.getIOManager().getSpillingDirectories();
		}
		else {
			List<File> dirs = new ArrayList<>(localDirectories.length);
			StringBuilder errorMessage = new StringBuilder();

			for (File f : localDirectories) {
				File testDir = new File(f, UUID.randomUUID().toString());
				if (!testDir.mkdirs()) {
					String msg = "Local files directory '" + f
						+ "' does not exist and cannot be created. ";
					LOG.error(msg);
					errorMessage.append(msg);
				} else {
					dirs.add(f);
				}
				//noinspection ResultOfMethodCallIgnored
				testDir.delete();
			}

			if (dirs.isEmpty()) {
				throw new IOException("No local storage directories available. " + errorMessage);
			} else {
				initializedBasePaths = dirs.toArray(new File[dirs.size()]);
			}
		}

		isInitialized = true;
	}

	private void initHeapStatusMonitor() {
		long checkInterval = configuration.get(SpillableOptions.HEAP_STATUS_CHECK_INTERVAL).toMillis();
		Preconditions.checkArgument(checkInterval > 0,
			"Heap status check interval should be larger than 0.");
		HeapStatusMonitor.initStatusMonitor(checkInterval);
	}
}
