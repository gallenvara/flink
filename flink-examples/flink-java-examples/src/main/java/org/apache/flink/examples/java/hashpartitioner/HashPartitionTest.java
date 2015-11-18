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

package org.apache.flink.examples.java.hashpartitioner;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class HashPartitionTest {

	private static String InputFile;


	public void testRangePartition() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(20);
		DataSet<Tuple3<String, Integer, Long>> dataSet = env.readCsvFile(InputFile).fieldDelimiter(" ").lineDelimiter("\n").includeFields("111").types(String.class, Integer.class, Long.class);
		/*DataSet<Tuple3<Integer, Long, String>> ds = env.readFile(new FileInputFormat<Tuple3<Integer, Long, String>>() {
			@Override
			public boolean reachedEnd() throws IOException {
				return false;
			}

			@Override
			public Tuple3<Integer, Long, String> nextRecord(Tuple3<Integer, Long, String> integerLongStringTuple3) throws IOException {
				return integerLongStringTuple3;
			}
		}, "/testInput/input1.txt");*/
		//DataSet<Integer> output = dataSet.partitionByRange(1).mapPartition(new StringMapper());
		env.setParallelism(1);
		DataSet<Tuple3<String, Integer, Long>> output = dataSet.partitionByHash(2).sortPartition(1, Order.DESCENDING);
		output.writeAsText("hdfs://HadoopMaster:9000/gaolun/output", FileSystem.WriteMode.OVERWRITE);
		env.execute("hash Partition");
	}

	public static void main(String[] args) throws Exception{
		InputFile = args[0];
		HashPartitionTest hashPartitionTest = new HashPartitionTest();
		hashPartitionTest.testRangePartition();
	}

	public static class StringMapper implements MapPartitionFunction<Tuple3<String, Integer, Long>, Integer> {
		private static final long serialVersionUID = 1L;

		@Override
		public void mapPartition(Iterable<Tuple3<String, Integer, Long>> records, Collector<Integer> out) throws Exception {
			HashSet<Long> uniq = new HashSet<Long>();
			int count = 0;
			for (Tuple3<String, Integer, Long> t : records) {
				count++;
			}
			out.collect(count);
			//for (Long l : uniq) {
			//	out.collect(l);
			//}
		}
	}
}
