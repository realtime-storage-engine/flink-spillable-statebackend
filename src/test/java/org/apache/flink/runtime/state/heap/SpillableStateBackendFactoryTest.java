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

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.filesystem.AbstractFileStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link SpillableStateBackendFactory}.
 */
public class SpillableStateBackendFactoryTest {

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	private final ClassLoader cl = getClass().getClassLoader();

	private final String backendKey = CheckpointingOptions.STATE_BACKEND.key();

	// ------------------------------------------------------------------------

	@Test
	public void testFactoryName() {
		// construct the name such that it will not be automatically adjusted on refactorings
		String factoryName = "org.apache.flink.runtime.state.heap.Spill";
		factoryName += "ableStateBackendFactory";

		// !!! if this fails, the code in StateBackendLoader must be adjusted
		assertEquals(factoryName, SpillableStateBackendFactory.class.getName());
	}

	/**
	 * Validates loading a file system state backend with additional parameters from the cluster configuration.
	 */
	@Test
	public void testLoadFileSystemStateBackend() throws Exception {
		final String checkpointDir = new Path(tmp.newFolder().toURI()).toString();
		final String savepointDir = new Path(tmp.newFolder().toURI()).toString();

		final Path expectedCheckpointsPath = new Path(checkpointDir);
		final Path expectedSavepointsPath = new Path(savepointDir);

		final Configuration config = new Configuration();
		config.setString(backendKey, SpillableStateBackendFactory.class.getName());
		config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);

		StateBackend backend = StateBackendLoader.loadStateBackendFromConfig(config, cl, null);

		assertTrue(backend instanceof SpillableStateBackend);

		SpillableStateBackend fs = (SpillableStateBackend) backend;

		AbstractFileStateBackend fsBackend = (AbstractFileStateBackend) fs.getCheckpointBackend();

		assertTrue(fsBackend instanceof FsStateBackend);

		assertEquals(expectedCheckpointsPath, fsBackend.getCheckpointPath());
		assertEquals(expectedSavepointsPath, fsBackend.getSavepointPath());
	}

	/**
	 * Validates loading a memory state backend with additional parameters from the cluster configuration.
	 */
	@Test
	public void testLoadMemoryStateBackend() throws Exception {
		final String savepointDir = new Path(tmp.newFolder().toURI()).toString();

		final Path expectedSavepointsPath = new Path(savepointDir);

		final Configuration config = new Configuration();
		config.setString(backendKey, SpillableStateBackendFactory.class.getName());
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);

		StateBackend backend = StateBackendLoader.loadStateBackendFromConfig(config, cl, null);

		assertTrue(backend instanceof SpillableStateBackend);

		SpillableStateBackend fs = (SpillableStateBackend) backend;

		AbstractFileStateBackend memBackend = (AbstractFileStateBackend) fs.getCheckpointBackend();

		assertTrue(memBackend instanceof MemoryStateBackend);

		assertNull(memBackend.getCheckpointPath());
		assertEquals(expectedSavepointsPath, memBackend.getSavepointPath());
	}

	/**
	 * Validates taking the application-defined file system state backend and adding with additional
	 * parameters from the cluster configuration, but giving precedence to application-defined
	 * parameters over configuration-defined parameters.
	 */
	@Test
	public void testLoadFileSystemStateBackendMixed() throws Exception {
		final String appCheckpointDir = new Path(tmp.newFolder().toURI()).toString();
		final String checkpointDir = new Path(tmp.newFolder().toURI()).toString();
		final String savepointDir = new Path(tmp.newFolder().toURI()).toString();

		final Path expectedCheckpointsPath = new Path(appCheckpointDir);
		final Path expectedSavepointsPath = new Path(savepointDir);

		final SpillableStateBackend backend = new SpillableStateBackend(appCheckpointDir);

		final Configuration config = new Configuration();
		config.setString(backendKey, "jobmanager"); // this should not be picked up
		config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir); // this should not be picked up
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);

		final StateBackend loadedBackend =
			StateBackendLoader.fromApplicationOrConfigOrDefault(backend, config, cl, null);
		assertTrue(loadedBackend instanceof SpillableStateBackend);

		final SpillableStateBackend loadedRocks = (SpillableStateBackend) loadedBackend;

		AbstractFileStateBackend fsBackend = (AbstractFileStateBackend) loadedRocks.getCheckpointBackend();
		assertEquals(expectedCheckpointsPath, fsBackend.getCheckpointPath());
		assertEquals(expectedSavepointsPath, fsBackend.getSavepointPath());
	}
}
