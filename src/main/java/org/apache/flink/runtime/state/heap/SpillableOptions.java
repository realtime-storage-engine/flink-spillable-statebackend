/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.heap.space.SpaceAllocator;

/**
 * Options for space allocation.
 */
public class SpillableOptions {

	/** Type of space used to create chunk. */
	public static final ConfigOption<String> SPACE_TYPE = ConfigOptions
		.key("state.backend.spillable.space-type")
		.defaultValue(SpaceAllocator.SpaceType.MMAP.name())
		.withDescription(String.format("Type of space used to create chunk. Options are %s (default), %s or %s.",
			SpaceAllocator.SpaceType.MMAP.name(), SpaceAllocator.SpaceType.HEAP.name(), SpaceAllocator.SpaceType.OFFHEAP.name()));

	/** Size of chunk. */
	public static final ConfigOption<MemorySize> CHUNK_SIZE = ConfigOptions
		.key("state.backend.spillable.chunk-size")
		.memoryType()
		.defaultValue(MemorySize.ofMebiBytes(512L))
		.withDescription("Size of chunk which should be a power of two and no more than Integer#MAX_VALUE.");

	/** Maximum number of mmap files that can be used. */
	public static final ConfigOption<Integer> MAX_MMAP_FILES = ConfigOptions
		.key("state.backend.spillable.max-mmap-files")
		.intType()
		.defaultValue(Integer.MAX_VALUE)
		.withDescription("Maximum number of mmap files that can be used.");

	/** Interval to check heap status. */
	public static final ConfigOption<Long> HEAP_STATUS_CHECK_INTERVAL = ConfigOptions
		.key("state.backend.spillable.heap-status.check-interval")
		.longType()
		.defaultValue(60000L)
		.withDescription("Interval to check heap status.");

	/** Threshold of gc time to trigger state spill.  */
	public static final ConfigOption<Long> GC_TIME_THRESHOLD = ConfigOptions
		.key("state.backend.spillable.gc-time.threshold")
		.longType()
		.defaultValue(2000L)
		.withDescription("If garbage collection time exceeds this threshold, state will be spilled.");

	/** Watermark under JVM heap usage is tried to control. */
	public static final ConfigOption<Float> HIGH_WATERMARK_RATIO = ConfigOptions
		.key("state.backend.spillable.high-watermark.ratio")
		.floatType()
		.defaultValue(0.5f)
		.withDescription("Watermark under which JVM heap usage is tried to control. Note this is not"
			+ " guaranteed if garbage collection implementation does not provide memory usage after gc.");

	/** Percentage of retained state size to spill in a turn. */
	public static final ConfigOption<Float> SPILL_SIZE_RATIO = ConfigOptions
		.key("state.backend.spillable.spill-size.ratio")
		.floatType()
		.defaultValue(0.2f)
		.withDescription("Percentage of retained state size to spill in a turn.");

	/** State load will be triggered if memory usage is under this watermark. */
	public static final ConfigOption<Float> LOAD_START_RATIO = ConfigOptions
		.key("state.backend.spillable.load-start.ratio")
		.floatType()
		.defaultValue(0.1f)
		.withDescription("State load will be triggered if memory usage is under this watermark.");

	/** Memory usage can't exceed this watermark after state load. */
	public static final ConfigOption<Float> LOAD_END_RATIO = ConfigOptions
		.key("state.backend.spillable.load-end.ratio")
		.floatType()
		.defaultValue(0.3f)
		.withDescription("Memory usage can't exceed this watermark after state load.");

	/** Interval between continuous spill/load.   */
	public static final ConfigOption<Long> TRIGGER_INTERVAL = ConfigOptions
		.key("state.backend.spillable.trigger-interval")
		.longType()
		.defaultValue(60000L)
		.withDescription("Interval to trigger continuous spill/load.");

	/** Interval to check resource. */
	public static final ConfigOption<Long> RESOURCE_CHECK_INTERVAL = ConfigOptions
		.key("state.backend.spillable.resource-check.interval")
		.longType()
		.defaultValue(10000L)
		.withDescription("Interval to check resource. High frequence will degrade performance but"
			+ " be more sensitive to memory change.");

	/** Whether to cancel checkpoint before spill. */
	public static final ConfigOption<Boolean> CANCEL_CHECKPOINT = ConfigOptions
		.key("state.backend.spillable.cancel.checkpoint")
		.booleanType()
		.defaultValue(true)
		.withDescription("Whether to cancel checkpoint before spill. Cancelling checkpoints will release " +
		"the spilled states and make them gc faster.");

	public static final ConfigOption[] SUPPORTED_CONFIG = new ConfigOption[]{
		SPACE_TYPE, CHUNK_SIZE, MAX_MMAP_FILES, HEAP_STATUS_CHECK_INTERVAL, GC_TIME_THRESHOLD, HIGH_WATERMARK_RATIO,
		SPILL_SIZE_RATIO, LOAD_START_RATIO, LOAD_END_RATIO, TRIGGER_INTERVAL, RESOURCE_CHECK_INTERVAL, CANCEL_CHECKPOINT
	};

	public static Configuration filter(ReadableConfig readableConfig) {
		Configuration conf = new Configuration();
		conf.set(SPACE_TYPE, readableConfig.get(SPACE_TYPE));
		conf.set(CHUNK_SIZE, readableConfig.get(CHUNK_SIZE));
		conf.set(MAX_MMAP_FILES, readableConfig.get(MAX_MMAP_FILES));
		conf.set(HEAP_STATUS_CHECK_INTERVAL, readableConfig.get(HEAP_STATUS_CHECK_INTERVAL));
		conf.set(GC_TIME_THRESHOLD, readableConfig.get(GC_TIME_THRESHOLD));
		conf.set(HIGH_WATERMARK_RATIO, readableConfig.get(HIGH_WATERMARK_RATIO));
		conf.set(SPILL_SIZE_RATIO, readableConfig.get(SPILL_SIZE_RATIO));
		conf.set(LOAD_START_RATIO, readableConfig.get(LOAD_START_RATIO));
		conf.set(LOAD_END_RATIO, readableConfig.get(LOAD_END_RATIO));
		conf.set(TRIGGER_INTERVAL, readableConfig.get(TRIGGER_INTERVAL));
		conf.set(RESOURCE_CHECK_INTERVAL, readableConfig.get(RESOURCE_CHECK_INTERVAL));
		conf.set(CANCEL_CHECKPOINT, readableConfig.get(CANCEL_CHECKPOINT));

		return conf;
	}
}
