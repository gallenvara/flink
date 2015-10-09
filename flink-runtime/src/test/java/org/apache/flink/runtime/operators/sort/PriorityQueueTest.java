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
	private static int QUEUE_SIZE = 1024 * 1024 * 32; 

	private static final int MEMORY_SIZE = 1024 * 1024 * 512;

	private static final int MEMORY_PAGE_SIZE = 32 * 1024;
	
	private static final int STRING_MAX_LENGTH = 9;

	private MemoryManager memoryManager;

	private TypeSerializer<IntPair> intPairSerializer;
	
	private TypeComparator<IntPair> intPairComparator;
	
	private TypeSerializer<IntStringPair> intStringPairSerializer;

	private TypeComparator<IntStringPair> intStringPairComparator;

	private StringSerializer serializer;

	private IntSerializer intSerializer;

	private IOManager ioManager;

	private static int[] fixData =new int[QUEUE_SIZE];

	private static String[] noFixData = new String[QUEUE_SIZE];

	private Random rand;
	

	@Setup
	public void init() {
		this.memoryManager = new MemoryManager(MEMORY_SIZE, MEMORY_PAGE_SIZE);
		this.ioManager = new IOManagerAsync();
		this.intPairSerializer = new IntPairSerializer();
		this.intPairComparator = new IntPairComparator();
		this.intStringPairComparator = new IntStringPairComparator(true);
		this.intStringPairSerializer = new IntStringPairSerializer();
		this.serializer = new StringSerializer();
		this.intSerializer = new IntSerializer();
		this.makeNoFixSizeData();
		this.makeFixSizeData();
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
	public void testQuickHeapWithFixData() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		this.rand = new Random();
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		QuickHeapPriorityQueue<IntPair> heap =
				new QuickHeapPriorityQueue<>(memory, intPairSerializer, intPairComparator, memoryManager, ioManager);
		
		try	{
			int count = 0;
			while (count < QUEUE_SIZE) {
				heap.insert(new IntPair(fixData[count], fixData[count]));
				count++;
			}
		}finally {
			heap.close();
		}
	}

	@Benchmark
	public void testQuickHeapWithNoFixData() throws Exception{
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		this.rand = new Random();
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		//IntStringPairComparator stringComparator2 = new IntStringPairComparator(false);
		QuickHeapPriorityQueue<IntStringPair> heap =
				new QuickHeapPriorityQueue<>(memory, intStringPairSerializer, intStringPairComparator, memoryManager, ioManager);

		try	{
			int count = 0;
			while (count < QUEUE_SIZE) {
				heap.insert(new IntStringPair(fixData[count], noFixData[count]));
				count++;
			}

		}finally {
			heap.close();
		}
	}

	@Benchmark
	public void testRadixHeapWithNoFixData() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		RadixHeapPriorityQueue<String> heap = new RadixHeapPriorityQueue<>(serializer, memory, ioManager);

		try {
			int count = 0;
			while (count < QUEUE_SIZE) {
				heap.insert(fixData[count], noFixData[count]);
				//System.out.println(noFixData[count] + "   " + fixData[count]);
				count++;
			}
		} finally {
			heap.close();
			this.memoryManager.release(heap.availableMemory);
		}
	}

	@Benchmark
	public void testRadixHeapWithFixData() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);
		RadixHeapPriorityQueue<Integer> heap = new RadixHeapPriorityQueue<>(intSerializer, memory, ioManager);

		try {
			int count = 0;
			while (count < QUEUE_SIZE) {
				heap.insert(fixData[count], fixData[count]);
				count++;
			}
		} finally {
			heap.close();
			this.memoryManager.release(heap.availableMemory);
		}
	}

	
	@Benchmark
	public void testJavaPriorityQueueWithFixData() {

		PriorityQueue<IntPair> queue = new PriorityQueue<>();
		try {
			int count = 0;
			while (count < QUEUE_SIZE) {
				queue.add(new IntPair(fixData[count], fixData[count]));
				count++;
			}
		}finally {
			queue.clear();
		}
	}
	
	@Benchmark
	public void testJavaPriorityQueueWithNoFixData() {

		PriorityQueue<IntStringPair> queue = new PriorityQueue<>();
		try {
			int count = 0;
			while (count < QUEUE_SIZE) {
				queue.add(new IntStringPair(fixData[count], noFixData[count]));
				count++;
			}
		}finally {
			queue.clear();
		}
	}
	
	public static void main(String[] args) throws RunnerException {
		
		Options opt = new OptionsBuilder()
				.include(PriorityQueueTest.class.getSimpleName())
				.warmupIterations(2)
				.measurementIterations(2)
				.forks(1)
				.build();
		new Runner(opt).run();
	}
}
