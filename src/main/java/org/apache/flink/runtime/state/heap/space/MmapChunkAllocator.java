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

package org.apache.flink.runtime.state.heap.space;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.state.heap.SpillableOptions;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.util.internal.PlatformDependent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of {@link ChunkAllocator} which is backed by memory mapped files.
 */
public class MmapChunkAllocator implements ChunkAllocator {

	private static final Logger LOG = LoggerFactory.getLogger(MmapChunkAllocator.class);

	/** name of directory to store mmap files. */
	public static final String MMAP_DIR = "spillable_mmap";

	/** Prefix name of mmap files. */
	private static final String MMAP_FILE_PREFIX = "mmap-";

	/** suffix of temporary files used to preallocate disk space for mmap files. */
	private static final String TMP_FILE_SUFFIX = ".tmp";

	/** Chunk size. */
	private final int chunkSize;

	/** Maximum number of mmap files that can be created. */
	private final int maxMmapFiles;

	/** Candidate directories to store mmap files. */
	private final List<CandidateDirectory> candidateDirectories;

	/** Collection of memory mapped files. */
	private final List<Tuple2<RandomAccessFile, MappedByteBuffer>> memoryMappedFiles;

	/** Number of mmap files created. */
	private int fileCount;

	private AtomicBoolean isClosed;

	public MmapChunkAllocator(int chunkSize, File[] localDirs, Configuration configuration) {
		this.chunkSize = chunkSize;

		this.maxMmapFiles = configuration.get(SpillableOptions.MAX_MMAP_FILES);
		Preconditions.checkArgument(maxMmapFiles >= 0, "Max mmap files must be" +
			" non-negative, but actually {}", maxMmapFiles);

		Preconditions.checkNotNull(localDirs, "Local dirs should not be null");
		Preconditions.checkArgument(localDirs.length > 0,
			"There should be at least one local directory to store mmap files.");
		this.candidateDirectories = prepareMmapDirectory(localDirs);
		this.memoryMappedFiles = new ArrayList<>();
		this.fileCount = 0;
		this.isClosed = new AtomicBoolean(false);
	}

	private List<CandidateDirectory> prepareMmapDirectory(File[] localDirs) {
		List<CandidateDirectory> mmapDirs = new ArrayList<>();
		for (File dir : localDirs) {
			File mmapDir = new File(dir, MMAP_DIR);

			if (mmapDir.exists()) {
				if (!mmapDir.isDirectory()) {
					removeCandidateDirectory(mmapDirs);
					throw new FlinkRuntimeException("Path to store mmap files is not a directory " + mmapDir.getAbsolutePath());
				}

				try {
					FileUtils.cleanDirectory(mmapDir);
				} catch (Exception e) {
					removeCandidateDirectory(mmapDirs);
					throw new FlinkRuntimeException("Failed to cleanup directory " + mmapDir.getAbsolutePath(), e);
				}
			} else {
				if (!mmapDir.mkdirs()) {
					removeCandidateDirectory(mmapDirs);
					throw new FlinkRuntimeException("Failed to create directory for mmap files: " + mmapDir);
				}
			}

			mmapDirs.add(new CandidateDirectory(mmapDir));
			if (LOG.isDebugEnabled()) {
				LOG.debug("MmapChunkAllocator use directory {}", mmapDir.getAbsolutePath());
			}
		}

		return mmapDirs;
	}

	@Override
	public Chunk createChunk(int chunkId, AllocateStrategy allocateStrategy) {

		if (fileCount >= maxMmapFiles) {
			throw new FlinkRuntimeException("Too many files created " + fileCount);
		}

		// use round-robin to balance directories
		int initDirIndex = fileCount % candidateDirectories.size();
		String fileName = MMAP_FILE_PREFIX + fileCount;

		// loop until mmapped buffer is created or we find that all directories are invalid
		while (true) {
			int invalidDirCount = 0;
			int dirIndex = initDirIndex;
			for (int i = 0; i < candidateDirectories.size(); i++) {
				CandidateDirectory mmapDir = candidateDirectories.get(dirIndex);
				if (!mmapDir.isCandidate()) {
					// if the directory is not a candidate, it must be invalid
					invalidDirCount++;
					continue;
				}

				try {
					Tuple2<RandomAccessFile, MappedByteBuffer> mmapFile =
						createMemoryMappedByteBuffer(new File(mmapDir.getPath(), fileName));
					memoryMappedFiles.add(mmapFile);
					mmapDir.markSuccess();
					fileCount++;
					return new DefaultChunkImpl(
						chunkId,
						MemorySegmentFactory.wrapOffHeapMemory(mmapFile.f1),
						allocateStrategy);
				} catch (Exception e) {
					mmapDir.markFail();
					if (!mmapDir.isValid()) {
						invalidDirCount++;
					}
					LOG.error("Failed to create MemoryMappedByteBuffer under {}", mmapDir.getPath(), e);
				}

				dirIndex++;
				if (dirIndex == candidateDirectories.size())  {
					dirIndex = 0;
				}
			}

			// all directories are invalid
			if (invalidDirCount == candidateDirectories.size()) {
				break;
			}
		}

		throw new FlinkRuntimeException("Failed to create chunk because all directories are invalid");
	}

	private Tuple2<RandomAccessFile, MappedByteBuffer> createMemoryMappedByteBuffer(File mmapFile) throws IOException {
		File tmpFile = new File(mmapFile.getAbsolutePath() + TMP_FILE_SUFFIX);

		if (mmapFile.exists() && !mmapFile.delete()) {
			LOG.error("Failed to delete mmap file {}", mmapFile.getAbsolutePath());
			throw new IOException("Failed to delete mmap file " + mmapFile.getAbsolutePath());
		}

		if (tmpFile.exists() && !tmpFile.delete()) {
			LOG.error("Failed to delete tmp file {}", tmpFile.getAbsolutePath());
			throw new IOException("Failed to delete tmp file " + tmpFile.getAbsolutePath());
		}

		RandomAccessFile mmapRafFile = null;
		RandomAccessFile tmpRafFile = null;
		try {
			tmpRafFile = new RandomAccessFile(tmpFile, "rw");
			tmpRafFile.setLength(chunkSize);

			try {
				mmapRafFile = new RandomAccessFile(mmapFile, "rw");
				FileChannel mmapChannel = mmapRafFile.getChannel();

				// Call transferTo to simulate fallocate on linux and reserve space for the mmaped file.
				// This is to avoid the case that if delayed write fails, OS will send SIGBUS signal to
				// JVM process. And since JVM process does not handle SIGBUS signal, it will be killed.
				tmpRafFile.getChannel().transferTo(0, chunkSize, mmapChannel);

				MappedByteBuffer buffer = mmapChannel.map(FileChannel.MapMode.READ_WRITE, 0L, chunkSize);
				return Tuple2.of(mmapRafFile, buffer);
			} catch (Exception e) {
				LOG.debug("Failed to create memory mapped byte buffer", e);

				if (mmapRafFile != null) {
					IOUtils.closeQuietly(mmapRafFile);
					if (!mmapFile.delete()) {
						LOG.warn("Failed to clean mmap file {}", mmapFile.getAbsolutePath());
					}
				}

				throw e;
			}
		} finally {
			if (tmpRafFile != null) {
				IOUtils.closeQuietly(tmpRafFile);
				if (!tmpFile.delete()) {
					LOG.warn("Failed to clean tmp file {}", tmpFile.getAbsolutePath());
				}
			}
		}
	}

	@Override
	public void close() {
		if (!isClosed.compareAndSet(false, true)) {
			return;
		}

		// unmap and close file
		for (Tuple2<RandomAccessFile, MappedByteBuffer> tuple : memoryMappedFiles) {
			PlatformDependent.freeDirectBuffer(tuple.f1);
			IOUtils.closeQuietly(tuple.f0);
		}

		// cleanup directories
		removeCandidateDirectory(candidateDirectories);
	}

	private void removeCandidateDirectory(List<CandidateDirectory> directories) {
		for (CandidateDirectory dir : directories) {
			try {
				FileUtils.deleteDirectory(dir.getPath());
			} catch (Exception e) {
				LOG.warn("Failed to cleanup directory {}", dir.getPath().getAbsolutePath());
			}
		}
	}

	/**
	 * Directory to store mmap files. The directory becomes invalid if it's marked failure
	 * more than {@link #FAIL_COUNT} continuously. And the directory can be a candidate if
	 * it's valid or {@link #INVALID_INTERVAL_MS} has been passed after it becomes invalid.
	 * {@link #INVALID_INTERVAL_MS} is useful in the case where the disk has a temporary failure.
	 */
	static class CandidateDirectory {

		/** An invalid directory can be a candidate after this interval has passed. */
		static final long INVALID_INTERVAL_MS = 600000L;

		/** Directory will become invalid after number of failures happen. */
		static final int FAIL_COUNT = 3;

		/** Directory path. */
		private File path;

		/** Whether this directory is valid. */
		private boolean isValid;

		/** Last time when this directory became invalid. */
		private long lastInvalidTime;

		/** Number of failures. */
		private int failCount;

		CandidateDirectory(File path) {
			this.path = path;
			this.isValid = true;
			this.lastInvalidTime = 0;
			this.failCount = 0;
		}

		File getPath() {
			return path;
		}

		boolean isCandidate() {
			return isValid || (System.currentTimeMillis() - lastInvalidTime > INVALID_INTERVAL_MS);
		}

		boolean isValid() {
			return isValid;
		}

		void markSuccess() {
			isValid = true;
			failCount = 0;
		}

		void markFail() {
			failCount++;
			if (failCount >= FAIL_COUNT) {
				isValid = false;
				lastInvalidTime = System.currentTimeMillis();
			}
		}

		@Override
		public String toString() {
			return "CandidateDirectory{" +
				"path=" + path +
				", isValid=" + isValid +
				", lastInvalidTime=" + lastInvalidTime +
				", failCount=" + failCount +
				'}';
		}
	}
}
