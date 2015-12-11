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

package org.apache.flink.api.scala.table.test

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.junit._

case class WC(word: String, count: Int)
class SqlExplainITCase {

  @Test
  def testGroupByWithoutExtended() : Unit = {
    val env = ExecutionEnvironment.createLocalEnvironment()
    val expr = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1)).toTable
    val result = expr.groupBy('word).select('word, 'count.sum as 'count).explain()
    val expected = """== Abstract Syntax Tree ==
Select(GroupBy(Root(ArraySeq((word,String), (count,Integer))), 'word), 'word,('count).sum as 'count)

== Physical Execution Plan ==
Stage 6 : Data Source
	content : at org.apache.flink.api.scala.ExecutionEnvironment.fromElements
	Partitioning : RANDOM_PARTITIONED

	Stage 5 : Map
		content : Map at select('word as 'word,'count as 'count)
		ship_strategy: Forward
		exchange_mode : PIPELINED
		driver_strategy : Map
		Partitioning : RANDOM_PARTITIONED

		Stage 4 : Map
			content : Map at select('word as 'word,'count as 'intermediate.1)
			ship_strategy: Forward
			exchange_mode : PIPELINED
			driver_strategy : Map
			Partitioning : RANDOM_PARTITIONED

			Stage 3 : GroupCombine
				content : GroupReduce at Expression Aggregation: Aggregate(GroupBy(Select(Root(ArraySeq((word,String), (count,Integer))), 'word as 'word,'count as 'intermediate.1), 'word), (intermediate.1,SUM))
				ship_strategy: Forward
				exchange_mode : PIPELINED
				driver_strategy : Sorted Combine
				Partitioning : RANDOM_PARTITIONED

				Stage 2 : GroupReduce
					content : GroupReduce at Expression Aggregation: Aggregate(GroupBy(Select(Root(ArraySeq((word,String), (count,Integer))), 'word as 'word,'count as 'intermediate.1), 'word), (intermediate.1,SUM))
					ship_strategy: Hash Partition on [0]
					local_strategy : Sort (combining) on [0:ASC]
					exchange_mode : PIPELINED
					driver_strategy : Sorted Group Reduce
					Partitioning : RANDOM_PARTITIONED

					Stage 1 : Map
						content : Map at select('word,'intermediate.1 as 'count)
						ship_strategy: Forward
						exchange_mode : PIPELINED
						driver_strategy : Map
						Partitioning : RANDOM_PARTITIONED

						Stage 0 : Data Sink
							content : org.apache.flink.api.java.io.DiscardingOutputFormat
							ship_strategy: Forward
							exchange_mode : PIPELINED
							Partitioning : RANDOM_PARTITIONED

"""
    assert(result.equals(expected))
  }

    @Test
  def testGroupByWithExtended() : Unit = {
    val env = ExecutionEnvironment.createLocalEnvironment()
    val expr = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1)).toTable
    val result = expr.groupBy('word).select('word, 'count.sum as 'count).explain(true)
    val expected = """== Abstract Syntax Tree ==
Select(GroupBy(Root(ArraySeq((word,String), (count,Integer))), 'word), 'word,('count).sum as 'count)

== Physical Execution Plan ==
Stage 6 : Data Source
	content : at org.apache.flink.api.scala.ExecutionEnvironment.fromElements
	Partitioning : RANDOM_PARTITIONED
	Partitioning Order : none
	Uniqueness : not unique
	Est. Output Size : unknow
	Est. Cardinality : unknow
	Network : 0.0
	Disk I/O : 0.0
	CPU : 0.0
	Cumulative Network : 0.0
	Cumulative Disk I/O : 0.0
	Cumulative CPU : 0.0
	Output Size (bytes) : none
	Output Cardinality : none
	Avg. Output Record Size (bytes) : none
	Filter Factor : none

	Stage 5 : Map
		content : Map at select('word as 'word,'count as 'count)
		ship_strategy: Forward
		exchange_mode : PIPELINED
		driver_strategy : Map
		Partitioning : RANDOM_PARTITIONED
		Partitioning Order : none
		Uniqueness : not unique
		Est. Output Size : unknow
		Est. Cardinality : unknow
		Network : 0.0
		Disk I/O : 0.0
		CPU : 0.0
		Cumulative Network : 0.0
		Cumulative Disk I/O : 0.0
		Cumulative CPU : 0.0
		Output Size (bytes) : none
		Output Cardinality : none
		Avg. Output Record Size (bytes) : none
		Filter Factor : none

		Stage 4 : Map
			content : Map at select('word as 'word,'count as 'intermediate.1)
			ship_strategy: Forward
			exchange_mode : PIPELINED
			driver_strategy : Map
			Partitioning : RANDOM_PARTITIONED
			Partitioning Order : none
			Uniqueness : not unique
			Est. Output Size : unknow
			Est. Cardinality : unknow
			Network : 0.0
			Disk I/O : 0.0
			CPU : 0.0
			Cumulative Network : 0.0
			Cumulative Disk I/O : 0.0
			Cumulative CPU : 0.0
			Output Size (bytes) : none
			Output Cardinality : none
			Avg. Output Record Size (bytes) : none
			Filter Factor : none

			Stage 3 : GroupCombine
				content : GroupReduce at Expression Aggregation: Aggregate(GroupBy(Select(Root(ArraySeq((word,String), (count,Integer))), 'word as 'word,'count as 'intermediate.1), 'word), (intermediate.1,SUM))
				ship_strategy: Forward
				exchange_mode : PIPELINED
				driver_strategy : Sorted Combine
				Partitioning : RANDOM_PARTITIONED
				Partitioning Order : none
				Uniqueness : not unique
				Est. Output Size : unknow
				Est. Cardinality : unknow
				Network : 0.0
				Disk I/O : 0.0
				CPU : 0.0
				Cumulative Network : 0.0
				Cumulative Disk I/O : 0.0
				Cumulative CPU : 0.0
				Output Size (bytes) : none
				Output Cardinality : none
				Avg. Output Record Size (bytes) : none
				Filter Factor : none

				Stage 2 : GroupReduce
					content : GroupReduce at Expression Aggregation: Aggregate(GroupBy(Select(Root(ArraySeq((word,String), (count,Integer))), 'word as 'word,'count as 'intermediate.1), 'word), (intermediate.1,SUM))
					ship_strategy: Hash Partition on [0]
					local_strategy : Sort (combining) on [0:ASC]
					exchange_mode : PIPELINED
					driver_strategy : Sorted Group Reduce
					Partitioning : RANDOM_PARTITIONED
					Partitioning Order : none
					Uniqueness : not unique
					Est. Output Size : unknow
					Est. Cardinality : unknow
					Network : unknown
					Disk I/O : unknown
					CPU : unknown
					Cumulative Network : unknown
					Cumulative Disk I/O : unknown
					Cumulative CPU : unknown
					Output Size (bytes) : none
					Output Cardinality : none
					Avg. Output Record Size (bytes) : none
					Filter Factor : none

					Stage 1 : Map
						content : Map at select('word,'intermediate.1 as 'count)
						ship_strategy: Forward
						exchange_mode : PIPELINED
						driver_strategy : Map
						Partitioning : RANDOM_PARTITIONED
						Partitioning Order : none
						Uniqueness : not unique
						Est. Output Size : unknow
						Est. Cardinality : unknow
						Network : 0.0
						Disk I/O : 0.0
						CPU : 0.0
						Cumulative Network : unknown
						Cumulative Disk I/O : unknown
						Cumulative CPU : unknown
						Output Size (bytes) : none
						Output Cardinality : none
						Avg. Output Record Size (bytes) : none
						Filter Factor : none

						Stage 0 : Data Sink
							content : org.apache.flink.api.java.io.DiscardingOutputFormat
							ship_strategy: Forward
							exchange_mode : PIPELINED
							Partitioning : RANDOM_PARTITIONED
							Partitioning Order : none
							Uniqueness : not unique
							Est. Output Size : unknow
							Est. Cardinality : unknow
							Network : 0.0
							Disk I/O : 0.0
							CPU : 0.0
							Cumulative Network : unknown
							Cumulative Disk I/O : unknown
							Cumulative CPU : unknown
							Output Size (bytes) : none
							Output Cardinality : none
							Avg. Output Record Size (bytes) : none
							Filter Factor : none

""" 
    assert(result.equals(expected))
  }
  
  @Test
  def testJoinWithoutExtended() : Unit = {
    val env = ExecutionEnvironment.createLocalEnvironment()
    val expr1 = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1)).toTable.as('a, 'b)
    val expr2 = env.fromElements(WC("hello", 1), WC("world", 1), WC("java", 2)).toTable.as('c, 'd)
    val result = expr1.join(expr2).where("b = d").select("a, c").explain()
    val expected = """== Abstract Syntax Tree ==
Select(Filter(Join(As(Root(ArraySeq((word,String), (count,Integer))), a,b), As(Root(ArraySeq((word,String), (count,Integer))), c,d)), 'b === 'd), 'a,'c)

== Physical Execution Plan ==
Stage 3 : Data Source
	content : at org.apache.flink.api.scala.ExecutionEnvironment.fromElements
	Partitioning : RANDOM_PARTITIONED

	Stage 2 : Map
		content : Map at select('word as 'word,'count as 'count)
		ship_strategy: Forward
		exchange_mode : PIPELINED
		driver_strategy : Map
		Partitioning : RANDOM_PARTITIONED

		Stage 5 : Data Source
			content : at org.apache.flink.api.scala.ExecutionEnvironment.fromElements
			Partitioning : RANDOM_PARTITIONED

			Stage 4 : Map
				content : Map at select('word as 'word,'count as 'count)
				ship_strategy: Forward
				exchange_mode : PIPELINED
				driver_strategy : Map
				Partitioning : RANDOM_PARTITIONED

				Stage 1 : Join
					content : Join at 'b === 'd
					side : first
					ship_strategy: Hash Partition on [1]
					exchange_mode : PIPELINED
					side : second
					ship_strategy: Hash Partition on [1]
					exchange_mode : PIPELINED
					driver_strategy : Hybrid Hash (build: Map at select('word as 'word,'count as 'count))
					Partitioning : RANDOM_PARTITIONED

					Stage 0 : Data Sink
						content : org.apache.flink.api.java.io.DiscardingOutputFormat
						ship_strategy: Forward
						exchange_mode : PIPELINED
						Partitioning : RANDOM_PARTITIONED

"""
    assert(result.equals(expected))
  }
  
  @Test
  def testJoinWithExtended() : Unit = {
    val env = ExecutionEnvironment.createLocalEnvironment()
    val expr1 = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1)).toTable.as('a, 'b)
    val expr2 = env.fromElements(WC("hello", 1), WC("world", 1), WC("java", 2)).toTable.as('c, 'd)
    val result = expr1.join(expr2).where("b = d").select("a, c").explain(true)
    val expected = """== Abstract Syntax Tree ==
Select(Filter(Join(As(Root(ArraySeq((word,String), (count,Integer))), a,b), As(Root(ArraySeq((word,String), (count,Integer))), c,d)), 'b === 'd), 'a,'c)

== Physical Execution Plan ==
Stage 3 : Data Source
	content : at org.apache.flink.api.scala.ExecutionEnvironment.fromElements
	Partitioning : RANDOM_PARTITIONED
	Partitioning Order : none
	Uniqueness : not unique
	Est. Output Size : unknow
	Est. Cardinality : unknow
	Network : 0.0
	Disk I/O : 0.0
	CPU : 0.0
	Cumulative Network : 0.0
	Cumulative Disk I/O : 0.0
	Cumulative CPU : 0.0
	Output Size (bytes) : none
	Output Cardinality : none
	Avg. Output Record Size (bytes) : none
	Filter Factor : none

	Stage 2 : Map
		content : Map at select('word as 'word,'count as 'count)
		ship_strategy: Forward
		exchange_mode : PIPELINED
		driver_strategy : Map
		Partitioning : RANDOM_PARTITIONED
		Partitioning Order : none
		Uniqueness : not unique
		Est. Output Size : unknow
		Est. Cardinality : unknow
		Network : 0.0
		Disk I/O : 0.0
		CPU : 0.0
		Cumulative Network : 0.0
		Cumulative Disk I/O : 0.0
		Cumulative CPU : 0.0
		Output Size (bytes) : none
		Output Cardinality : none
		Avg. Output Record Size (bytes) : none
		Filter Factor : none

		Stage 5 : Data Source
			content : at org.apache.flink.api.scala.ExecutionEnvironment.fromElements
			Partitioning : RANDOM_PARTITIONED
			Partitioning Order : none
			Uniqueness : not unique
			Est. Output Size : unknow
			Est. Cardinality : unknow
			Network : 0.0
			Disk I/O : 0.0
			CPU : 0.0
			Cumulative Network : 0.0
			Cumulative Disk I/O : 0.0
			Cumulative CPU : 0.0
			Output Size (bytes) : none
			Output Cardinality : none
			Avg. Output Record Size (bytes) : none
			Filter Factor : none

			Stage 4 : Map
				content : Map at select('word as 'word,'count as 'count)
				ship_strategy: Forward
				exchange_mode : PIPELINED
				driver_strategy : Map
				Partitioning : RANDOM_PARTITIONED
				Partitioning Order : none
				Uniqueness : not unique
				Est. Output Size : unknow
				Est. Cardinality : unknow
				Network : 0.0
				Disk I/O : 0.0
				CPU : 0.0
				Cumulative Network : 0.0
				Cumulative Disk I/O : 0.0
				Cumulative CPU : 0.0
				Output Size (bytes) : none
				Output Cardinality : none
				Avg. Output Record Size (bytes) : none
				Filter Factor : none

				Stage 1 : Join
					content : Join at 'b === 'd
					side : first
					ship_strategy: Hash Partition on [1]
					exchange_mode : PIPELINED
					side : second
					ship_strategy: Hash Partition on [1]
					exchange_mode : PIPELINED
					driver_strategy : Hybrid Hash (build: Map at select('word as 'word,'count as 'count))
					Partitioning : RANDOM_PARTITIONED
					Partitioning Order : none
					Uniqueness : not unique
					Est. Output Size : unknow
					Est. Cardinality : unknow
					Network : unknown
					Disk I/O : unknown
					CPU : unknown
					Cumulative Network : unknown
					Cumulative Disk I/O : unknown
					Cumulative CPU : unknown
					Output Size (bytes) : none
					Output Cardinality : none
					Avg. Output Record Size (bytes) : none
					Filter Factor : none

					Stage 0 : Data Sink
						content : org.apache.flink.api.java.io.DiscardingOutputFormat
						ship_strategy: Forward
						exchange_mode : PIPELINED
						Partitioning : RANDOM_PARTITIONED
						Partitioning Order : none
						Uniqueness : not unique
						Est. Output Size : unknow
						Est. Cardinality : unknow
						Network : 0.0
						Disk I/O : 0.0
						CPU : 0.0
						Cumulative Network : unknown
						Cumulative Disk I/O : unknown
						Cumulative CPU : unknown
						Output Size (bytes) : none
						Output Cardinality : none
						Avg. Output Record Size (bytes) : none
						Filter Factor : none

"""
    assert(result.equals(expected))
  }
  
  @Test
  def testUnionWithoutExtended() : Unit = {
    val env = ExecutionEnvironment.createLocalEnvironment()
    val expr1 = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1)).toTable
    val expr2 = env.fromElements(WC("hello", 1), WC("world", 1), WC("java", 2)).toTable
    val result = expr1.unionAll(expr2).explain()
    val expected = """== Abstract Syntax Tree ==
Union(Root(ArraySeq((word,String), (count,Integer))), Root(ArraySeq((word,String), (count,Integer))))

== Physical Execution Plan ==
Stage 3 : Data Source
	content : at org.apache.flink.api.scala.ExecutionEnvironment.fromElements
	Partitioning : RANDOM_PARTITIONED

	Stage 2 : Map
		content : Map at select('word as 'word,'count as 'count)
		ship_strategy: Forward
		exchange_mode : PIPELINED
		driver_strategy : Map
		Partitioning : RANDOM_PARTITIONED

		Stage 5 : Data Source
			content : at org.apache.flink.api.scala.ExecutionEnvironment.fromElements
			Partitioning : RANDOM_PARTITIONED

			Stage 4 : Map
				content : Map at select('word as 'word,'count as 'count)
				ship_strategy: Forward
				exchange_mode : PIPELINED
				driver_strategy : Map
				Partitioning : RANDOM_PARTITIONED

				Stage 1 : Union
					content : 
					side : first
					ship_strategy: Redistribute
					exchange_mode : PIPELINED
					side : second
					ship_strategy: Redistribute
					exchange_mode : PIPELINED
					Partitioning : RANDOM_PARTITIONED

					Stage 0 : Data Sink
						content : org.apache.flink.api.java.io.DiscardingOutputFormat
						ship_strategy: Forward
						exchange_mode : PIPELINED
						Partitioning : RANDOM_PARTITIONED

"""
    assert(result.equals(expected))
  }
  
  @Test
  def testUnionWithExtended() : Unit = {
    val env = ExecutionEnvironment.createLocalEnvironment()
    val expr1 = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1)).toTable
    val expr2 = env.fromElements(WC("hello", 1), WC("world", 1), WC("java", 2)).toTable
    val result = expr1.unionAll(expr2).explain(true)
    val expected = """== Abstract Syntax Tree ==
Union(Root(ArraySeq((word,String), (count,Integer))), Root(ArraySeq((word,String), (count,Integer))))

== Physical Execution Plan ==
Stage 3 : Data Source
	content : at org.apache.flink.api.scala.ExecutionEnvironment.fromElements
	Partitioning : RANDOM_PARTITIONED
	Partitioning Order : none
	Uniqueness : not unique
	Est. Output Size : unknow
	Est. Cardinality : unknow
	Network : 0.0
	Disk I/O : 0.0
	CPU : 0.0
	Cumulative Network : 0.0
	Cumulative Disk I/O : 0.0
	Cumulative CPU : 0.0
	Output Size (bytes) : none
	Output Cardinality : none
	Avg. Output Record Size (bytes) : none
	Filter Factor : none

	Stage 2 : Map
		content : Map at select('word as 'word,'count as 'count)
		ship_strategy: Forward
		exchange_mode : PIPELINED
		driver_strategy : Map
		Partitioning : RANDOM_PARTITIONED
		Partitioning Order : none
		Uniqueness : not unique
		Est. Output Size : unknow
		Est. Cardinality : unknow
		Network : 0.0
		Disk I/O : 0.0
		CPU : 0.0
		Cumulative Network : 0.0
		Cumulative Disk I/O : 0.0
		Cumulative CPU : 0.0
		Output Size (bytes) : none
		Output Cardinality : none
		Avg. Output Record Size (bytes) : none
		Filter Factor : none

		Stage 5 : Data Source
			content : at org.apache.flink.api.scala.ExecutionEnvironment.fromElements
			Partitioning : RANDOM_PARTITIONED
			Partitioning Order : none
			Uniqueness : not unique
			Est. Output Size : unknow
			Est. Cardinality : unknow
			Network : 0.0
			Disk I/O : 0.0
			CPU : 0.0
			Cumulative Network : 0.0
			Cumulative Disk I/O : 0.0
			Cumulative CPU : 0.0
			Output Size (bytes) : none
			Output Cardinality : none
			Avg. Output Record Size (bytes) : none
			Filter Factor : none

			Stage 4 : Map
				content : Map at select('word as 'word,'count as 'count)
				ship_strategy: Forward
				exchange_mode : PIPELINED
				driver_strategy : Map
				Partitioning : RANDOM_PARTITIONED
				Partitioning Order : none
				Uniqueness : not unique
				Est. Output Size : unknow
				Est. Cardinality : unknow
				Network : 0.0
				Disk I/O : 0.0
				CPU : 0.0
				Cumulative Network : 0.0
				Cumulative Disk I/O : 0.0
				Cumulative CPU : 0.0
				Output Size (bytes) : none
				Output Cardinality : none
				Avg. Output Record Size (bytes) : none
				Filter Factor : none

				Stage 1 : Union
					content : 
					side : first
					ship_strategy: Redistribute
					exchange_mode : PIPELINED
					side : second
					ship_strategy: Redistribute
					exchange_mode : PIPELINED
					Partitioning : RANDOM_PARTITIONED
					Partitioning Order : none
					Uniqueness : not unique
					Est. Output Size : unknow
					Est. Cardinality : unknow
					Network : 0.0
					Disk I/O : 0.0
					CPU : 0.0
					Cumulative Network : unknown
					Cumulative Disk I/O : 0.0
					Cumulative CPU : 0.0
					Output Size (bytes) : none
					Output Cardinality : none
					Avg. Output Record Size (bytes) : none
					Filter Factor : none

					Stage 0 : Data Sink
						content : org.apache.flink.api.java.io.DiscardingOutputFormat
						ship_strategy: Forward
						exchange_mode : PIPELINED
						Partitioning : RANDOM_PARTITIONED
						Partitioning Order : none
						Uniqueness : not unique
						Est. Output Size : unknow
						Est. Cardinality : unknow
						Network : 0.0
						Disk I/O : 0.0
						CPU : 0.0
						Cumulative Network : unknown
						Cumulative Disk I/O : 0.0
						Cumulative CPU : 0.0
						Output Size (bytes) : none
						Output Cardinality : none
						Avg. Output Record Size (bytes) : none
						Filter Factor : none

"""
    assert(result.equals(expected))
  }
}
