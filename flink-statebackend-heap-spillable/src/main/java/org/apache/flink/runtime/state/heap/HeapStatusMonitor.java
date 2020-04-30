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

import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ShutdownHookUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Monitor memory usage and garbage collection.
 */
public class HeapStatusMonitor {

	private static final Logger LOG = LoggerFactory.getLogger(HeapStatusMonitor.class);

	/** Single instance in a JVM process. */
	private static HeapStatusMonitor statusMonitor;

	/** Interval to check memory usage. */
	private final long checkIntervalInMs;

	private final MemoryMXBean memoryMXBean;

	private final long maxMemory;

	private final List<GarbageCollectorMXBean> garbageCollectorMXBeans;

	/** Generate ascending id for each monitor result. */
	private final AtomicLong resultIdGenerator;

	/** Executor to check memory usage periodically. */
	private final ScheduledThreadPoolExecutor checkExecutor;

	private final ScheduledFuture checkFuture;

	/** The latest monitor result. */
	private volatile MonitorResult monitorResult;

	/** Time for gc when last check. */
	private long lastGcTime;

	/** Number of gc when last check. */
	private long lastGcCount;

	/** Flag to signify that the monitor has been shut down already. */
	private final AtomicBoolean isShutdown = new AtomicBoolean();

	/** Shutdown hook to make sure that scheduler is closed. */
	private final Thread shutdownHook;

	HeapStatusMonitor(long checkIntervalInMs) {
		Preconditions.checkArgument(checkIntervalInMs > 0, "Check interval should be positive.");
		this.checkIntervalInMs = checkIntervalInMs;
		this.memoryMXBean = ManagementFactory.getMemoryMXBean();
		this.maxMemory = memoryMXBean.getHeapMemoryUsage().getMax();
		this.garbageCollectorMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
		this.resultIdGenerator = new AtomicLong(0L);
		this.monitorResult = new MonitorResult(System.currentTimeMillis(), resultIdGenerator.getAndIncrement(),
			memoryMXBean.getHeapMemoryUsage(), 0);
		this.lastGcTime = 0L;
		this.lastGcCount = 0L;

		this.shutdownHook = ShutdownHookUtil.addShutdownHook(this::shutDown, getClass().getSimpleName(), LOG);

		this.checkExecutor = new ScheduledThreadPoolExecutor(1, new ExecutorThreadFactory("memory-status-monitor"));
		this.checkExecutor.setRemoveOnCancelPolicy(true);
		this.checkExecutor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
		this.checkExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

		this.checkFuture = this.checkExecutor.scheduleWithFixedDelay(this::runCheck, 10, checkIntervalInMs, TimeUnit.MILLISECONDS);

		LOG.info("Max memory {}, Check interval {}", maxMemory, checkIntervalInMs);
	}

	private void runCheck() {
		long timestamp = System.currentTimeMillis();
		long id = resultIdGenerator.getAndIncrement();
		this.monitorResult = new MonitorResult(timestamp, id, memoryMXBean.getHeapMemoryUsage(), getGarbageCollectionTime());
		if (LOG.isDebugEnabled()) {
			LOG.debug("Check memory status, {}", monitorResult.toString());
		}
	}

	private long getGarbageCollectionTime() {
		long count = 0;
		long timeMillis = 0;
		for (GarbageCollectorMXBean gcBean : garbageCollectorMXBeans) {
			long c = gcBean.getCollectionCount();
			long t = gcBean.getCollectionTime();
			count += c;
			timeMillis += t;
		}

		if (count == lastGcCount) {
			return 0;
		}

		long gcCountIncrement = count - lastGcCount;
		long averageGcTime = (timeMillis - lastGcTime) / gcCountIncrement;

		lastGcCount = count;
		lastGcTime = timeMillis;

		return averageGcTime;
	}

	public MonitorResult getMonitorResult() {
		return monitorResult;
	}

	public long getMaxMemory() {
		return maxMemory;
	}

	void shutDown() {
		if (!isShutdown.compareAndSet(false, true)) {
			return;
		}

		ShutdownHookUtil.removeShutdownHook(shutdownHook, getClass().getSimpleName(), LOG);

		if (checkFuture != null) {
			checkFuture.cancel(true);
		}

		if (checkExecutor != null) {
			checkExecutor.shutdownNow();
		}

		LOG.info("Memory monitor is shutdown.");
	}

	public static HeapStatusMonitor getStatusMonitor() {
		return statusMonitor;
	}

	public static void initStatusMonitor(long checkIntervalInMs) {
		synchronized (HeapStatusMonitor.class) {
			if (statusMonitor != null) {
				return;
			}

			statusMonitor = new HeapStatusMonitor(checkIntervalInMs);
		}
	}

	/**
	 * Monitor result.
	 */
	static class MonitorResult {

		/** Time of status. */
		private final long timestamp;

		/** Unique id of status. */
		private final long id;

		private final long totalMemory;

		private final long totalUsedMemory;

		private final long garbageCollectionTime;

		MonitorResult(long timestamp, long id, MemoryUsage memoryUsage, long garbageCollectionTime) {
			this(timestamp, id, memoryUsage.getMax(), memoryUsage.getUsed(), garbageCollectionTime);
		}

		MonitorResult(long timestamp, long id, long totalMemory, long totalUsedMemory, long garbageCollectionTime) {
			this.timestamp = timestamp;
			this.id = id;
			this.totalMemory = totalMemory;
			this.totalUsedMemory = totalUsedMemory;
			this.garbageCollectionTime = garbageCollectionTime;
		}

		public long getTimestamp() {
			return timestamp;
		}

		public long getId() {
			return id;
		}

		public long getTotalMemory() {
			return totalMemory;
		}

		public long getTotalUsedMemory() {
			return totalUsedMemory;
		}

		public long getGarbageCollectionTime() {
			return garbageCollectionTime;
		}

		@Override
		public String toString() {
			return "MonitorResult{" +
				"timestamp=" + timestamp +
				", id=" + id +
				", totalMemory=" + totalMemory +
				", totalUsedMemory=" + totalUsedMemory +
				", garbageCollectionTime=" + garbageCollectionTime +
				'}';
		}
	}
}
