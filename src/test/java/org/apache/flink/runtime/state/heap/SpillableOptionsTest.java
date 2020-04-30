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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;

import org.junit.Test;
import org.powermock.core.IdentityHashSet;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link SpillableOptions}.
 */
public class SpillableOptionsTest {

	@Test
	public void testSupportedConfig() throws Exception {
		IdentityHashSet<ConfigOption> optionSet = getOptionsViaReflection();

		assertEquals(optionSet.size(), SpillableOptions.SUPPORTED_CONFIG.length);
		for (ConfigOption option : SpillableOptions.SUPPORTED_CONFIG) {
			assertTrue(optionSet.contains(option));
		}
	}

	@Test
	public void testFilterOptions() throws Exception {
		IdentityHashSet<ConfigOption> optionSet = getOptionsViaReflection();

		Set<String> keySet = SpillableOptions.filter(new Configuration()).toMap().keySet();
		assertEquals(optionSet.size(), keySet.size());
		for (ConfigOption option : optionSet) {
			assertTrue(keySet.contains(option.key()));
		}
	}

	private IdentityHashSet<ConfigOption> getOptionsViaReflection() throws Exception {
		IdentityHashSet<ConfigOption> set = new IdentityHashSet<>();
		Field[] fields = SpillableOptions.class.getDeclaredFields();
		for (Field f : fields) {
			if (Modifier.isStatic(f.getModifiers()) && f.getType() == ConfigOption.class) {
				set.add((ConfigOption) f.get(null));
			}
		}

		return set;
	}
}
