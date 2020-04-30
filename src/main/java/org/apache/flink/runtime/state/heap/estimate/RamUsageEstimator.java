/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap.estimate;

import org.apache.flink.api.common.state.State;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;

/**
 * Estimates the size (memory representation) of Java objects.
 *
 * <p>This class uses assumptions that were discovered for the Hotspot
 * virtual machine. If you use a non-OpenJDK/Oracle-based JVM,
 * the measurements may be slightly wrong.
 * Refer to https://github.com/apache/lucene-solr/blob/master/lucene/core/src/java/org/apache/lucene/util/RamUsageEstimator.java.
 */
public final class RamUsageEstimator {

	/** One kilobyte bytes. */
	public static final long ONE_KB = 1024;

	/** One megabyte bytes. */
	public static final long ONE_MB = ONE_KB * ONE_KB;

	/** One gigabyte bytes.*/
	public static final long ONE_GB = ONE_KB * ONE_MB;

	/** No instantiation. */
	private RamUsageEstimator() {}

	/**
	 * True, iff compressed references (oops) are enabled by this JVM.
	 */
	public static final boolean COMPRESSED_REFS_ENABLED;

	/**
	 * Number of bytes this JVM uses to represent an object reference.
	 */
	public static final  int NUM_BYTES_OBJECT_REF;

	/**
	 * Number of bytes to represent an object header (no fields, no alignments).
	 */
	public static final int NUM_BYTES_OBJECT_HEADER;

	/**
	 * Number of bytes to represent an array header (no content, but with alignments).
	 */
	public static final int NUM_BYTES_ARRAY_HEADER;

	/**
	 * A constant specifying the object alignment boundary inside the JVM. Objects will
	 * always take a full multiple of this constant, possibly wasting some space.
	 */
	public static final int NUM_BYTES_OBJECT_ALIGNMENT;

	/**
	 * Approximate memory usage that we assign to all unknown objects -
	 * this maps roughly to a few primitive fields and a couple short String-s.
	 */
	public static final int UNKNOWN_DEFAULT_RAM_BYTES_USED = 256;

	/**
	 * Sizes of primitive classes.
	 */
	public static final Map<Class<?>, Integer> PRIMITIVE_SIZES;

	static {
		Map<Class<?>, Integer> primitiveSizesMap = new IdentityHashMap<>();
		primitiveSizesMap.put(boolean.class, 1);
		primitiveSizesMap.put(byte.class, 1);
		primitiveSizesMap.put(char.class, Integer.valueOf(Character.BYTES));
		primitiveSizesMap.put(short.class, Integer.valueOf(Short.BYTES));
		primitiveSizesMap.put(int.class, Integer.valueOf(Integer.BYTES));
		primitiveSizesMap.put(float.class, Integer.valueOf(Float.BYTES));
		primitiveSizesMap.put(double.class, Integer.valueOf(Double.BYTES));
		primitiveSizesMap.put(long.class, Integer.valueOf(Long.BYTES));

		PRIMITIVE_SIZES = Collections.unmodifiableMap(primitiveSizesMap);
	}

	/**
	 * JVMs typically cache small longs. This tries to find out what the range is.
	 */
	static final int LONG_SIZE, STRING_SIZE;

	/** For testing only. */
	static final boolean JVM_IS_HOTSPOT_64BIT;

	static final String MANAGEMENT_FACTORY_CLASS = "java.lang.management.ManagementFactory";
	static final String HOTSPOT_BEAN_CLASS = "com.sun.management.HotSpotDiagnosticMXBean";

	/**
	 * Initialize constants and try to collect information about the JVM internals.
	 */
	static {
		if (Constants.JRE_IS_64BIT) {
			// Try to get compressed oops and object alignment (the default seems to be 8 on Hotspot);
			// (this only works on 64 bit, on 32 bits the alignment and reference size is fixed):
			boolean compressedOops = false;
			int objectAlignment = 8;
			boolean isHotspot = false;
			try {
				final Class<?> beanClazz = Class.forName(HOTSPOT_BEAN_CLASS);
				// we use reflection for this, because the management factory is not part
				// of Java 8's compact profile:
				final Object hotSpotBean = Class.forName(MANAGEMENT_FACTORY_CLASS)
					.getMethod("getPlatformMXBean", Class.class)
					.invoke(null, beanClazz);
				if (hotSpotBean != null) {
					isHotspot = true;
					final Method getVMOptionMethod = beanClazz.getMethod("getVMOption", String.class);
					try {
						final Object vmOption = getVMOptionMethod.invoke(hotSpotBean, "UseCompressedOops");
						compressedOops = Boolean.parseBoolean(
							vmOption.getClass().getMethod("getValue").invoke(vmOption).toString()
						);
					} catch (ReflectiveOperationException | RuntimeException e) {
						isHotspot = false;
					}
					try {
						final Object vmOption = getVMOptionMethod.invoke(hotSpotBean, "ObjectAlignmentInBytes");
						objectAlignment = Integer.parseInt(
							vmOption.getClass().getMethod("getValue").invoke(vmOption).toString()
						);
					} catch (ReflectiveOperationException | RuntimeException e) {
						isHotspot = false;
					}
				}
			} catch (ReflectiveOperationException | RuntimeException e) {
				isHotspot = false;
			}
			JVM_IS_HOTSPOT_64BIT = isHotspot;
			COMPRESSED_REFS_ENABLED = compressedOops;
			NUM_BYTES_OBJECT_ALIGNMENT = objectAlignment;
			// reference size is 4, if we have compressed oops:
			NUM_BYTES_OBJECT_REF = COMPRESSED_REFS_ENABLED ? 4 : 8;
			// "best guess" based on reference size:
			NUM_BYTES_OBJECT_HEADER = 8 + NUM_BYTES_OBJECT_REF;
			// array header is NUM_BYTES_OBJECT_HEADER + NUM_BYTES_INT, but aligned (object alignment):
			NUM_BYTES_ARRAY_HEADER = (int) alignObjectSize(NUM_BYTES_OBJECT_HEADER + Integer.BYTES);
		} else {
			JVM_IS_HOTSPOT_64BIT = false;
			COMPRESSED_REFS_ENABLED = false;
			NUM_BYTES_OBJECT_ALIGNMENT = 8;
			NUM_BYTES_OBJECT_REF = 4;
			NUM_BYTES_OBJECT_HEADER = 8;
			// For 32 bit JVMs, no extra alignment of array header:
			NUM_BYTES_ARRAY_HEADER = NUM_BYTES_OBJECT_HEADER + Integer.BYTES;
		}

		LONG_SIZE = (int) shallowSizeOfInstance(Long.class);
		STRING_SIZE = (int) shallowSizeOfInstance(String.class);
	}

	/** Approximate memory usage that we assign to a Hashtable / HashMap entry. */
	public static final long HASHTABLE_RAM_BYTES_PER_ENTRY =
		2 * NUM_BYTES_OBJECT_REF // key + value
			* 2; // hash tables need to be oversized to avoid collisions, assume 2x capacity

	/** Approximate memory usage that we assign to a LinkedHashMap entry. */
	public static final long LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY =
		HASHTABLE_RAM_BYTES_PER_ENTRY
			+ 2 * NUM_BYTES_OBJECT_REF; // previous & next references

	/**
	 * Aligns an object size to be the next multiple of {@link #NUM_BYTES_OBJECT_ALIGNMENT}.
	 */
	public static long alignObjectSize(long size) {
		size += (long) NUM_BYTES_OBJECT_ALIGNMENT - 1L;
		return size - (size % NUM_BYTES_OBJECT_ALIGNMENT);
	}

	/**
	 * Return the size of the provided {@link Long} object, returning 0 if it is
	 * cached by the JVM and its shallow size otherwise.
	 */
	public static long sizeOf(Long value) {
		return LONG_SIZE;
	}

	/** Returns the size in bytes of the byte[] object. */
	public static long sizeOf(byte[] arr) {
		return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + arr.length);
	}

	/** Returns the size in bytes of the boolean[] object. */
	public static long sizeOf(boolean[] arr) {
		return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + arr.length);
	}

	/** Returns the size in bytes of the char[] object. */
	public static long sizeOf(char[] arr) {
		return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) Character.BYTES * arr.length);
	}

	/** Returns the size in bytes of the short[] object. */
	public static long sizeOf(short[] arr) {
		return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) Short.BYTES * arr.length);
	}

	/** Returns the size in bytes of the int[] object. */
	public static long sizeOf(int[] arr) {
		return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) Integer.BYTES * arr.length);
	}

	/** Returns the size in bytes of the float[] object. */
	public static long sizeOf(float[] arr) {
		return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) Float.BYTES * arr.length);
	}

	/** Returns the size in bytes of the long[] object. */
	public static long sizeOf(long[] arr) {
		return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) Long.BYTES * arr.length);
	}

	/** Returns the size in bytes of the double[] object. */
	public static long sizeOf(double[] arr) {
		return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) Double.BYTES * arr.length);
	}

	/** Returns the size in bytes of the String[] object. */
	public static long sizeOf(String[] arr) {
		long size = shallowSizeOf(arr);
		for (String s : arr) {
			if (s == null) {
				continue;
			}
			size += sizeOf(s);
		}
		return size;
	}

	/** Returns the size in bytes of the String object. */
	public static long sizeOf(String s) {
		if (s == null) {
			return 0;
		}
		// may not be true in Java 9+ and CompactStrings - but we have no way to determine this

		// char[] + hashCode
		long size = STRING_SIZE + (long) NUM_BYTES_ARRAY_HEADER + (long) Character.BYTES * s.length();
		return alignObjectSize(size);
	}

	/** Same as calling <code>sizeOf(obj, DEFAULT_FILTER)</code>. */
	public static long sizeOf(Object obj) {
		return sizeOf(obj, new Accumulator());
	}

	/**
	 * Estimates the RAM usage by the given object. It will
	 * walk the object tree and sum up all referenced objects.
	 *
	 * <p><b>Resource Usage:</b> This method internally uses a set of
	 * every object seen during traversals so it does allocate memory
	 * (it isn't side-effect free). After the method exits, this memory
	 * should be GCed.</p>
	 */
	public static long sizeOf(Object obj, Accumulator accumulator) {
		return measureObjectSize(obj, accumulator);
	}

	/** Returns the shallow size in bytes of the Object[] object. */
	// Use this method instead of #shallowSizeOf(Object) to avoid costly reflection
	public static long shallowSizeOf(Object[] arr) {
		return alignObjectSize((long) NUM_BYTES_ARRAY_HEADER + (long) NUM_BYTES_OBJECT_REF * arr.length);
	}

	/**
	 * Estimates a "shallow" memory usage of the given object. For arrays, this will be the
	 * memory taken by array storage (no subreferences will be followed). For objects, this
	 * will be the memory taken by the fields.
	 * JVM object alignments are also applied.
	 */
	public static long shallowSizeOf(Object obj) {
		if (obj == null) {
			return 0;
		}
		final Class<?> clz = obj.getClass();
		if (clz.isArray()) {
			return shallowSizeOfArray(obj);
		} else {
			return shallowSizeOfInstance(clz);
		}
	}

	/**
	 * Returns the shallow instance size in bytes an instance of the given class would occupy.
	 * This works with all conventional classes and primitive types, but not with arrays
	 * (the size then depends on the number of elements and varies from object to object).
	 *
	 * @see #shallowSizeOf(Object)
	 * @throws IllegalArgumentException if {@code clazz} is an array class.
	 */
	public static long shallowSizeOfInstance(Class<?> clazz) {
		if (clazz.isArray()) {
			throw new IllegalArgumentException("This method does not work with array classes.");
		}
		if (clazz.isPrimitive()) {
			return PRIMITIVE_SIZES.get(clazz);
		}

		long size = NUM_BYTES_OBJECT_HEADER;

		// Walk type hierarchy
		for (; clazz != null; clazz = clazz.getSuperclass()) {
			final Class<?> target = clazz;
			final Field[] fields = AccessController.doPrivileged(new PrivilegedAction<Field[]>() {
				@Override
				public Field[] run() {
					return target.getDeclaredFields();
				}
			});
			for (Field f : fields) {
				if (!Modifier.isStatic(f.getModifiers())) {
					size = adjustForField(size, f);
				}
			}
		}
		return alignObjectSize(size);
	}

	/**
	 * Return shallow size of any <code>array</code>.
	 */
	private static long shallowSizeOfArray(Object array) {
		long size = NUM_BYTES_ARRAY_HEADER;
		final int len = Array.getLength(array);
		if (len > 0) {
			Class<?> arrayElementClazz = array.getClass().getComponentType();
			if (arrayElementClazz.isPrimitive()) {
				size += (long) len * PRIMITIVE_SIZES.get(arrayElementClazz);
			} else {
				size += (long) NUM_BYTES_OBJECT_REF * len;
			}
		}
		return alignObjectSize(size);
	}

	/**
	 * This method returns the maximum representation size of an object. <code>sizeSoFar</code>
	 * is the object's size measured so far. <code>f</code> is the field being probed.
	 *
	 * <p>The returned offset will be the maximum of whatever was measured so far and
	 * <code>f</code> field's offset and representation size (unaligned).
	 */
	static long adjustForField(long sizeSoFar, final Field f) {
		final Class<?> type = f.getType();
		final int fsize = type.isPrimitive() ? PRIMITIVE_SIZES.get(type) : NUM_BYTES_OBJECT_REF;
		// TODO: No alignments based on field type/ subclass fields alignments?
		return sizeSoFar + fsize;
	}

	/**
	 * Returns <code>size</code> in human-readable units (GB, MB, KB or bytes).
	 */
	public static String humanReadableUnits(long bytes) {
		return humanReadableUnits(bytes,
			new DecimalFormat("0.#", DecimalFormatSymbols.getInstance(Locale.ROOT)));
	}

	/**
	 * Returns <code>size</code> in human-readable units (GB, MB, KB or bytes).
	 */
	public static String humanReadableUnits(long bytes, DecimalFormat df) {
		if (bytes / ONE_GB > 0) {
			return df.format((float) bytes / ONE_GB) + " GB";
		} else if (bytes / ONE_MB > 0) {
			return df.format((float) bytes / ONE_MB) + " MB";
		} else if (bytes / ONE_KB > 0) {
			return df.format((float) bytes / ONE_KB) + " KB";
		} else {
			return bytes + " bytes";
		}
	}

	/**
	 * Return a human-readable size of a given object.
	 * @see #sizeOf(Object)
	 * @see RamUsageEstimator#humanReadableUnits(long)
	 */
	public static String humanSizeOf(Object object) {
		return RamUsageEstimator.humanReadableUnits(sizeOf(object));
	}

	/**
	 * An accumulator of object references. This class allows for customizing RAM usage estimation.
	 */
	public static class Accumulator {

		private static final int DEFAULT_MAX_DEEP = 10;

		private final int maxDeep;

		public Accumulator() {
			this(DEFAULT_MAX_DEEP);
		}

		public Accumulator(int maxDeep) {
			this.maxDeep = maxDeep;
		}

		/**
		 * Accumulate transitive references for the provided fields of the given
		 * object into <code>queue</code> and return the shallow size of this object.
		 */
		public long accumulateObject(
			Object o,
			long shallowSize,
			Map<Field, Object> fieldValues,
			Collection<ObjectWithDeep> queue,
			int deep) {
			if (deep < maxDeep) {
				for (Object object : fieldValues.values()) {
					queue.add(new ObjectWithDeep(object, deep + 1));
				}
			}
			return shallowSize;
		}

		/**
		 * Accumulate transitive references for the provided values of the given
		 * array into <code>queue</code> and return the shallow size of this array.
		 */
		public long accumulateArray(
			Object array,
			long shallowSize,
			List<Object> values,
			Collection<ObjectWithDeep> queue,
			int deep) {
			if (deep < maxDeep) {
				for (Object object : values) {
					queue.add(new ObjectWithDeep(object, deep + 1));
				}
			}
			return shallowSize;
		}
	}

	private static class ObjectWithDeep {
		private Object object;
		private int deep;

		public ObjectWithDeep(Object ob, int deep) {
			this.object = ob;
			this.deep = deep;
		}

		public Object getObject() {
			return object;
		}

		public int getDeep() {
			return deep;
		}
	}

	/**
	 * Non-recursive version of object descend. This consumes more memory than recursive in-depth
	 * traversal but prevents stack overflows on long chains of objects
	 * or complex graphs (a max. recursion depth on my machine was ~5000 objects linked in a chain
	 * so not too much).
	 */
	private static long measureObjectSize(Object root, Accumulator accumulator) {
		// Objects seen so far.
		final Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>());
		// Class cache with reference Field and precalculated shallow size.
		final IdentityHashMap<Class<?>, ClassCache> classCache = new IdentityHashMap<>();
		// Stack of objects pending traversal. Recursion caused stack overflows.
		final ArrayList<ObjectWithDeep> stack = new ArrayList<>();
		stack.add(new ObjectWithDeep(root, 0));

		long totalSize = 0;
		while (!stack.isEmpty()) {
			final ObjectWithDeep objectWithDeep = stack.remove(stack.size() - 1);
			final Object ob = objectWithDeep.getObject();
			final int currentDeep = objectWithDeep.getDeep();

			// skip the state object
			if (ob == null || ob instanceof State || seen.contains(ob)) {
				continue;
			}
			seen.add(ob);

			final long obSize;
			final Class<?> obClazz = ob.getClass();
			assert obClazz != null : "jvm bug detected (Object.getClass() == null). please report this to your vendor";
			if (obClazz.isArray()) {
				obSize = handleArray(accumulator, stack, ob, obClazz, currentDeep);
			} else {
				obSize = handleOther(accumulator, classCache, stack, ob, obClazz, currentDeep);
			}

			totalSize += obSize;
			// Dump size of each object for comparisons across JVMs and flags.
			// System.out.println("  += " + obClazz + " | " + obSize);
		}

		// Help the GC (?).
		seen.clear();
		stack.clear();
		classCache.clear();

		return totalSize;
	}

	private static long handleOther(
		Accumulator accumulator,
		IdentityHashMap<Class<?>,
		ClassCache> classCache,
		ArrayList<ObjectWithDeep> stack,
		Object ob,
		Class<?> obClazz,
		int deep) {
		/*
		 * Consider an object. Push any references it has to the processing stack
		 * and accumulate this object's shallow size.
		 */
		try {
			if (Constants.JRE_IS_MINIMUM_JAVA9) {
				long alignedShallowInstanceSize = RamUsageEstimator.shallowSizeOf(ob);

				Predicate<Class<?>> isJavaModule = (clazz) -> clazz.getName().startsWith("java.");

				// Java 9: Best guess for some known types, as we cannot precisely look into runtime classes
				final ToLongFunction<Object> func = SIMPLE_TYPES.get(obClazz);
				if (func != null) {
					// some simple type like String where the size is easy to get from public properties
					return accumulator.accumulateObject(
						ob, alignedShallowInstanceSize + func.applyAsLong(ob), Collections.emptyMap(), stack, deep);
				} else if (ob instanceof Enum) {
					return alignedShallowInstanceSize;
				} else if (ob instanceof ByteBuffer) {
					// Approximate ByteBuffers with their underlying storage (ignores field overhead).
					return byteArraySize(((ByteBuffer) ob).capacity());
				} else {
					// Fallback to reflective access.
				}
			}

			ClassCache cachedInfo = classCache.get(obClazz);
			if (cachedInfo == null) {
				classCache.put(obClazz, cachedInfo = createCacheEntry(obClazz));
			}

			final Map<Field, Object> fieldValues = new HashMap<>();
			for (Field f : cachedInfo.referenceFields) {
				fieldValues.put(f, f.get(ob));
			}
			return accumulator.accumulateObject(ob, cachedInfo.alignedShallowInstanceSize, fieldValues, stack, deep);
		} catch (IllegalAccessException e) {
			// this should never happen as we enabled setAccessible().
			throw new RuntimeException("Reflective field access failed?", e);
		}
	}

	private static long handleArray(
		Accumulator accumulator,
		ArrayList<ObjectWithDeep> stack,
		Object ob,
		Class<?> obClazz,
		int deep) {
		/*
		 * Consider an array, possibly of primitive types. Push any of its references to
		 * the processing stack and accumulate this array's shallow size.
		 */
		final long shallowSize = RamUsageEstimator.shallowSizeOf(ob);
		final int len = Array.getLength(ob);
		final List<Object> values;
		Class<?> componentClazz = obClazz.getComponentType();
		if (componentClazz.isPrimitive()) {
			values = Collections.emptyList();
		} else {
			values = new AbstractList<Object>() {

				@Override
				public Object get(int index) {
					return Array.get(ob, index);
				}

				@Override
				public int size() {
					return len;
				}
			};
		}
		return accumulator.accumulateArray(ob, shallowSize, values, stack, deep);
	}

	/**
	 * This map contains a function to calculate sizes of some "simple types" like String just from their public properties.
	 * This is needed for Java 9, which does not allow to look into runtime class fields.
	 */
	@SuppressWarnings("serial")
	private static final Map<Class<?>, ToLongFunction<Object>> SIMPLE_TYPES =
		Collections.unmodifiableMap(new IdentityHashMap<Class<?>, ToLongFunction<Object>>() {
			{ init(); }

			private void init() {
				// String types:
				a(String.class, v -> charArraySize(v.length())); // may not be correct with Java 9's compact strings!
				a(StringBuilder.class, v -> charArraySize(v.capacity()));
				a(StringBuffer.class, v -> charArraySize(v.capacity()));
				// Types with large buffers:
				a(ByteArrayOutputStream.class, v -> byteArraySize(v.size()));
				// For File and Path, we just take the length of String representation as approximation:
				a(File.class, v -> charArraySize(v.toString().length()));
				a(Path.class, v -> charArraySize(v.toString().length()));
				a(ByteOrder.class, v -> 0); // Instances of ByteOrder are constants
			}

			@SuppressWarnings("unchecked")
			private <T> void a(Class<T> clazz, ToLongFunction<T> func) {
				put(clazz, (ToLongFunction<Object>) func);
			}

			private long charArraySize(int len) {
				return RamUsageEstimator.alignObjectSize((long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) Character.BYTES * len);
			}
		});

	/**
	 * Cached information about a given class.
	 */
	private static final class ClassCache {
		public final long alignedShallowInstanceSize;
		public final Field[] referenceFields;

		public ClassCache(long alignedShallowInstanceSize, Field[] referenceFields) {
			this.alignedShallowInstanceSize = alignedShallowInstanceSize;
			this.referenceFields = referenceFields;
		}
	}

	/**
	 * Create a cached information about shallow size and reference fields for a given class.
	 */
	private static ClassCache createCacheEntry(final Class<?> clazz) {
		return AccessController.doPrivileged((PrivilegedAction<ClassCache>) () -> {
			ClassCache cachedInfo;
			long shallowInstanceSize = RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
			final ArrayList<Field> referenceFields = new ArrayList<>(32);
			for (Class<?> c = clazz; c != null; c = c.getSuperclass()) {
				if (c == Class.class) {
					// prevent inspection of Class' fields, throws SecurityException in Java 9!
					continue;
				}
				final Field[] fields = c.getDeclaredFields();
				for (final Field f : fields) {
					if (!Modifier.isStatic(f.getModifiers())) {
						shallowInstanceSize = RamUsageEstimator.adjustForField(shallowInstanceSize, f);

						if (!f.getType().isPrimitive()) {
							try {
								f.setAccessible(true);
								referenceFields.add(f);
							} catch (RuntimeException re) {
								throw new RuntimeException(String.format(Locale.ROOT,
									"Can't access field '%s' of class '%s' for RAM estimation.",
									f.getName(),
									clazz.getName()), re);
							}
						}
					}
				}
			}

			cachedInfo = new ClassCache(RamUsageEstimator.alignObjectSize(shallowInstanceSize),
				referenceFields.toArray(new Field[referenceFields.size()]));
			return cachedInfo;
		});
	}

	private static long byteArraySize(int len) {
		return RamUsageEstimator.alignObjectSize((long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + len);
	}
}
