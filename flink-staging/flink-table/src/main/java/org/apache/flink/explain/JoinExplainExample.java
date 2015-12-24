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

package org.apache.flink.explain;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.TableEnvironment;
import org.apache.flink.api.table.Table;

public class JoinExplainExample {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		System.out.println(env.getClass());
		TableEnvironment tableEnv = new TableEnvironment();
		DataSet<WC> ds1 = env.fromElements(
				new WC("hello", 1),
				new WC("world", 3),
				new WC("nian", 5));
		Table table1 = tableEnv.fromDataSet(ds1).as("a, b");
		DataSet<WC> ds2 = env.fromElements(
				new WC("hello", 2),
				new WC("java", 3),
				new WC("world", 5));
		Table table2 = tableEnv.fromDataSet(ds2).as("c, d");
		System.out.println(table1.join(table2).where("b = d").select("a, c").explain());
	}

	public static class WC {
		public String word;
		public int count;
		public WC(String word, int count) {
			this.word = word;
			this.count = count;
		}
		public WC() {}
	}
}
