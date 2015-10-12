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
package org.apache.flink.runtime.operators.sort;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.types.*;
import org.junit.Assert;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.TimeUnit;


@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class PriorityQueueTest {
	private static int QUEUE_SIZE; 

	private static final int MEMORY_SIZE = 1024 * 1024 * 512;

	private static final int MEMORY_PAGE_SIZE = 32 * 1024;
	
	private static final int STRING_MAX_LENGTH = 9;

	private MemoryManager memoryManager;

	private static int[] fixData =new int[QUEUE_SIZE];

	private static String[] noFixData = new String[QUEUE_SIZE];

	private Random rand;

	private static QuickHeapPriorityQueue<IntPair> intPairQuickHeapQueue;
	
	private static QuickHeapPriorityQueue<IntStringPair> intStringPairQuickHeapQueue;
	
	private static RadixHeapPriorityQueue<Integer> intPairRadixHeapQueue;

	private static RadixHeapPriorityQueue<String> intStringPairRadixHeapQueue;
	
	private static PriorityQueue<IntPair> intPairPriorityQueue;
	
	private static PriorityQueue<IntStringPair> intStringPairPriorityQueue;
	

	@Setup
	public void init() throws Exception{
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		this.memoryManager = new MemoryManager(MEMORY_SIZE, MEMORY_PAGE_SIZE);
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);

		IOManager ioManager = new IOManagerAsync();
		TypeSerializer<IntPair> intPairSerializer = new IntPairSerializer();
		TypeComparator<IntPair> intPairComparator = new IntPairComparator();
		TypeComparator<IntStringPair> intStringPairComparator = new IntStringPairComparator(true);
		TypeSerializer<IntStringPair> intStringPairSerializer = new IntStringPairSerializer();
		StringSerializer serializer = new StringSerializer();
		IntSerializer intSerializer = new IntSerializer();
		
		this.makeNoFixSizeData();
		this.makeFixSizeData();
		
		intPairQuickHeapQueue = new QuickHeapPriorityQueue<>(memory, intPairSerializer, intPairComparator, memoryManager, ioManager);
		intStringPairQuickHeapQueue = new QuickHeapPriorityQueue<>(memory, intStringPairSerializer, intStringPairComparator, memoryManager, ioManager);
		intPairRadixHeapQueue = new RadixHeapPriorityQueue<>(intSerializer, memory, ioManager);
		intStringPairRadixHeapQueue = new RadixHeapPriorityQueue<>(serializer, memory, ioManager);
		intPairPriorityQueue = new PriorityQueue<>();
		intStringPairPriorityQueue = new PriorityQueue<>();
		
	}

	@TearDown
	public void afterTest() {
		if (!this.memoryManager.verifyEmpty()) {
			Assert.fail("Memory Leak: Some memory has not been returned to the memory manager.");
		}

		if (this.memoryManager != null) {
			this.memoryManager.shutdown();
			this.memoryManager = null;
		}
	}
	
	public void makeFixSizeData() {
		this.rand = new Random();
			for (int i = 0; i < QUEUE_SIZE; i++)
			{
				fixData[i] = rand.nextInt(Integer.MAX_VALUE);
			}
	}
	
	public void makeNoFixSizeData() {
		this.rand = new Random();
			for (int i = 0; i < QUEUE_SIZE; i++) 
			{
				noFixData[i] = RandomStringUtils.randomAlphanumeric(rand.nextInt(STRING_MAX_LENGTH) + 1);
			}
	}

	@Benchmark
	public void testQuickHeapWithWriteFixData() throws Exception {
			int count = 0;
			while (count < QUEUE_SIZE) {
				intPairQuickHeapQueue.insert(new IntPair(fixData[count], fixData[count]));
				count++;
			}
	}
	
	@Benchmark
	public void testQuickHeapWithReadFixData() throws Exception {
		if (intPairQuickHeapQueue.size() == 0) {
			this.testQuickHeapWithWriteFixData();
		} else {
			try {
				int count = 0;
				IntPair element;
				while (count < QUEUE_SIZE) {
					element = intPairQuickHeapQueue.next();
					//System.out.println(element.getKey() + "  " + element.getValue());
					count++;
				}
			} finally {
				intPairQuickHeapQueue.close();
			}
		}
	}

	@Benchmark
	public void testQuickHeapWithWriteNoFixData() throws Exception{
		
		
			int count = 0;
			while (count < QUEUE_SIZE) {
				intStringPairQuickHeapQueue.insert(new IntStringPair(fixData[count], noFixData[count]));
				count++;
			}
	}

	@Benchmark
	public void testQuickHeapWithReadNoFixData() throws Exception {
		if (intStringPairQuickHeapQueue.size() == 0) {
			this.testQuickHeapWithWriteNoFixData();
		} else {
			try {
				int count = 0;
				IntStringPair element;
				while (count < QUEUE_SIZE) {
					element = intStringPairQuickHeapQueue.next();
					//System.out.println(element.getKey() + "  " + element.getValue());
					count++;
				}
			} finally {
				intStringPairQuickHeapQueue.close();
			}
		}
	}

	@Benchmark
	public void testRadixHeapWithWriteNoFixData() throws Exception {


			int count = 0;
			while (count < QUEUE_SIZE) {
				intStringPairRadixHeapQueue.insert(fixData[count], noFixData[count]);
				//System.out.println(noFixData[count] + "   " + fixData[count]);
				count++;
			}
	}
	
	@Benchmark
	public void testRadixHeapWithReadNoFixData() throws Exception {
		if (intStringPairRadixHeapQueue.poll() == null) {
			this.testRadixHeapWithWriteNoFixData();
		} else {
			try {
				int count = 0;
				String element;
				while (count < QUEUE_SIZE) {
					element = intStringPairRadixHeapQueue.poll();
					//System.out.println(element);
					count++;
				}
			} finally {
				intStringPairRadixHeapQueue.close();
				this.memoryManager.release(intStringPairRadixHeapQueue.availableMemory);
			}
		}
	}

	@Benchmark
	public void testRadixHeapWithWriteFixData() throws Exception {
			int count = 0;
			while (count < QUEUE_SIZE) {
				intPairRadixHeapQueue.insert(fixData[count], fixData[count]);
				count++;
			}
	}

	@Benchmark
	public void testRadixHeapWithReadFixData() throws Exception {

		if (intPairRadixHeapQueue.poll() == null) {
			this.testRadixHeapWithWriteFixData();
		} else {
			try {
				int count = 0;
				int element;
				while (count < QUEUE_SIZE) {
					element = intPairRadixHeapQueue.poll();
					//System.out.println(element);
					count++;
				}
			} finally {
				intPairRadixHeapQueue.close();
				this.memoryManager.release(intPairRadixHeapQueue.availableMemory);
			}
		}
	}

	
	@Benchmark
	public void testJavaPriorityQueueWithWriteFixData() {
		
			int count = 0;
			while (count < QUEUE_SIZE) {
				intPairPriorityQueue.add(new IntPair(fixData[count], fixData[count]));
				count++;
			}
		
	}
	
	@Benchmark
	public void testJavaPriorityQueueWithReadFixData() throws Exception{
		if (intPairPriorityQueue.isEmpty()) {
			this.testJavaPriorityQueueWithWriteFixData();
		} else {
			try {
				int count = 0;
				IntPair element;
				while (count < QUEUE_SIZE) {
					element = intPairPriorityQueue.poll();
					//System.out.println(element);
					count++;
				}
			} finally {
				intPairPriorityQueue.clear();
			}
		}
	}
	
	@Benchmark
	public void testJavaPriorityQueueWithWriteNoFixData() {
		
			int count = 0;
			while (count < QUEUE_SIZE) {
				intStringPairPriorityQueue.add(new IntStringPair(fixData[count], noFixData[count]));
				count++;
			}
	}
	
	@Benchmark
	public void testJavaPriorityQueueWithReadNoFixData() {
		if (intStringPairPriorityQueue.isEmpty()) {
			this.testJavaPriorityQueueWithWriteNoFixData();
		} else {
			try {
				int count = 0;
				IntStringPair element;
				while (count < QUEUE_SIZE) {
					element = intStringPairPriorityQueue.poll();
					//System.out.println(element);
					count++;
				}
			} finally {
				intStringPairPriorityQueue.clear();
			}
		}
	}
	
	public static void main(int sizeNum) throws RunnerException {
		
		QUEUE_SIZE = 1024 * 1024 * sizeNum;
		Options opt = new OptionsBuilder()
				.include(PriorityQueueTest.class.getSimpleName())
				.warmupIterations(2)
				.measurementIterations(2)
				.forks(1)
				.build();
		new Runner(opt).run();
	}
}
