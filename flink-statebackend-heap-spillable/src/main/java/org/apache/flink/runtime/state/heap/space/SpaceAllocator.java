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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.heap.SpillableOptions;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.state.heap.space.SpaceConstants.BUCKET_SIZE;
import static org.apache.flink.runtime.state.heap.space.SpaceConstants.FOUR_BYTES_BITS;
import static org.apache.flink.runtime.state.heap.space.SpaceConstants.FOUR_BYTES_MARK;
import static org.apache.flink.runtime.state.heap.space.SpaceConstants.NO_SPACE;

/**
 * An implementation of {@link Allocator} which is not thread safe.
 */
public class SpaceAllocator implements Allocator {

	/**
	 * Array of all chunks.
	 */
	private volatile Chunk[] totalSpace = new Chunk[16];

	/**
	 * List of chunks used to allocate space for data less than bucket size.
	 */
	private final List<Chunk> totalSpaceForNormal = new ArrayList<>();

	/**
	 * List of chunks used to allocate space for data bigger than or equal to bucket size.
	 */
	private final List<Chunk> totalSpaceForHuge = new ArrayList<>();

	/**
	 * Generator for chunk id.
	 */
	private final AtomicInteger chunkIdGenerator = new AtomicInteger(0);

	/**
	 * The chunk allocator.
	 */
	private final ChunkAllocator chunkAllocator;

	public SpaceAllocator(Configuration configuration, @Nullable File[] localDirs) {
		this.chunkAllocator = createChunkAllocator(configuration, localDirs);
	}

	@VisibleForTesting
	SpaceAllocator(ChunkAllocator chunkAllocator) {
		this.chunkAllocator = Preconditions.checkNotNull(chunkAllocator);
	}

	@VisibleForTesting
	void addTotalSpace(Chunk chunk, int chunkId) {
		if (chunkId >= this.totalSpace.length) {
			Chunk[] chunkTemp = new Chunk[this.totalSpace.length * 2];
			System.arraycopy(this.totalSpace, 0, chunkTemp, 0, this.totalSpace.length);
			this.totalSpace = chunkTemp;
		}
		totalSpace[chunkId] = chunk;
	}

	@Override
	public long allocate(int len) {
		if (len >= BUCKET_SIZE) {
			return doAllocate(totalSpaceForHuge, len, AllocateStrategy.HugeBucket);
		} else {
			return doAllocate(totalSpaceForNormal, len, AllocateStrategy.SmallBucket);
		}
	}

	@Override
	public void free(long offset) {
		int chunkId = SpaceUtils.getChunkIdByAddress(offset);
		int interChunkOffset = SpaceUtils.getChunkOffsetByAddress(offset);
		getChunkById(chunkId).free(interChunkOffset);
	}

	@Override
	public Chunk getChunkById(int chunkId) {
		return totalSpace[chunkId];
	}

	private long doAllocate(List<Chunk> chunks, int len, AllocateStrategy allocateStrategy) {
		int offset;
		for (Chunk chunk : chunks) {
			offset = chunk.allocate(len);
			if (offset != NO_SPACE) {
				return ((chunk.getChunkId() & FOUR_BYTES_MARK) << FOUR_BYTES_BITS) | (offset & FOUR_BYTES_MARK);
			}
		}

		Chunk chunk = createChunk(allocateStrategy);
		chunks.add(chunk);
		addTotalSpace(chunk, chunk.getChunkId());
		offset = chunk.allocate(len);

		if (offset != NO_SPACE) {
			return ((chunk.getChunkId() & FOUR_BYTES_MARK) << FOUR_BYTES_BITS) | (offset & FOUR_BYTES_MARK);
		}

		throw new RuntimeException("There is no space to allocate");
	}

	private Chunk createChunk(AllocateStrategy allocateStrategy) {
		int chunkId = chunkIdGenerator.getAndIncrement();
		return chunkAllocator.createChunk(chunkId, allocateStrategy);
	}

	@Override
	public void close() throws IOException {
		chunkAllocator.close();
	}

	@VisibleForTesting
	ChunkAllocator getChunkAllocator() {
		return chunkAllocator;
	}

	@VisibleForTesting
	AtomicInteger getChunkIdGenerator() {
		return chunkIdGenerator;
	}


	/**
	 * Type of space where to allocate chunks.
	 */
	public enum SpaceType {

		/**
		 * Allocates chunks from heap. This is mainly used for test.
		 */
		HEAP,

		/**
		 * Allocates chunks from off-heap.
		 */
		OFFHEAP,

		/**
		 * Allocates chunks from mmp.
		 */
		MMAP
	}

	private static ChunkAllocator createChunkAllocator(Configuration configuration, @Nullable File[] localDirs) {
		SpaceType spaceType = SpaceType.valueOf(configuration.get(SpillableOptions.SPACE_TYPE).toUpperCase());
		long chunkSize = configuration.get(SpillableOptions.CHUNK_SIZE).getBytes();
		Preconditions.checkArgument(chunkSize <= Integer.MAX_VALUE,
			"Chunk size should be less than Integer.MAX_VALUE, but is actually %s", chunkSize);
		Preconditions.checkArgument(MathUtils.isPowerOf2(chunkSize),
			"Chunk size should be a power of two, but is actually %s", chunkSize);

		switch (spaceType) {
			case HEAP:
				return new HeapBufferChunkAllocator((int) chunkSize);
			case OFFHEAP:
				return new DirectBufferChunkAllocator((int) chunkSize);
			case MMAP:
				return new MmapChunkAllocator((int) chunkSize, localDirs, configuration);
			default:
				throw new UnsupportedOperationException("Unsupported space type " + spaceType.name());
		}
	}
}
