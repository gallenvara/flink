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

package org.apache.flink.examples.java.partitioner;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class RangePartitionTest {

	
	public void testRangePartition() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<String, Integer, Long>> dataSet = env.readCsvFile("file:///testdata/textfile.txt").fieldDelimiter(" ").lineDelimiter("\n").includeFields("111").types(String.class, Integer.class, Long.class);
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
		DataSet<Integer> output = dataSet.partitionByRange(1).mapPartition(new StringMapper());
		output.writeAsText("file:///testdata/output1.txt", FileSystem.WriteMode.OVERWRITE);
		env.setParallelism(4);
		env.execute("range Partition");
	}
	
	public static void main(String[] args) throws Exception{
		RangePartitionTest rangePartitionTest = new RangePartitionTest();
		rangePartitionTest.testRangePartition();
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
