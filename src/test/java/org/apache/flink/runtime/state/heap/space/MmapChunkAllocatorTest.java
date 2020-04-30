/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.runtime.state.heap.space;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

/**
 * Tests for {@link MmapChunkAllocator}.
 */
public class MmapChunkAllocatorTest {

	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testPrepareDirectory() throws Exception {
		File[] dirs = new File[3];
		for (int i = 0; i < dirs.length; i++) {
			dirs[i] = tempFolder.newFolder();
		}

		MmapChunkAllocator allocator =
			new MmapChunkAllocator(10240, dirs, new Configuration());
		for (File dir : dirs) {
			Assert.assertTrue(new File(dir, MmapChunkAllocator.MMAP_DIR).exists());
		}

		allocator.close();
	}

	@Test
	public void testPrepareDirectoryFailed() throws Exception {
		File[] dirs = new File[3];
		dirs[0] = tempFolder.newFolder();
		dirs[1] = tempFolder.newFolder();
		// deliver a file to make preparation failed
		dirs[2] = tempFolder.newFile();

		try {
			new MmapChunkAllocator(10240, dirs, new Configuration());
			Assert.fail("Should fail because of delivering a file");
		} catch (Exception e) {
			Assert.assertTrue(e instanceof FlinkRuntimeException);
		}

		Assert.assertFalse(new File(dirs[0], MmapChunkAllocator.MMAP_DIR).exists());
		Assert.assertFalse(new File(dirs[1], MmapChunkAllocator.MMAP_DIR).exists());
		Assert.assertTrue(dirs[2].exists());
	}
}
