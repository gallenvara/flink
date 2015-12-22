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
    val source = scala.io.Source.fromFile("C:\\Users\\lungao\\mygit\\flink\\flink-staging" +
      "\\flink-table\\src\\test\\testFile\\testGroupBy0.out").mkString
    assert(result.equals(source))
  }

    @Test
  def testGroupByWithExtended() : Unit = {
    val env = ExecutionEnvironment.createLocalEnvironment()
    val expr = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1)).toTable
    val result = expr.groupBy('word).select('word, 'count.sum as 'count).explain(true)
      val source = scala.io.Source.fromFile("C:\\Users\\lungao\\mygit\\flink\\flink-staging" +
        "\\flink-table\\src\\test\\testFile\\testGroupBy1.out").mkString
      assert(result.equals(source))

  }
  
  @Test
  def testJoinWithoutExtended() : Unit = {
    val env = ExecutionEnvironment.createLocalEnvironment()
    val expr1 = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1)).toTable.as('a, 'b)
    val expr2 = env.fromElements(WC("hello", 1), WC("world", 1), WC("java", 2)).toTable.as('c, 'd)
    val result = expr1.join(expr2).where("b = d").select("a, c").explain()
    val source = scala.io.Source.fromFile("C:\\Users\\lungao\\mygit\\flink\\flink-staging" +
      "\\flink-table\\src\\test\\testFile\\testJoin0.out").mkString
    assert(result.equals(source))
  }
  
  @Test
  def testJoinWithExtended() : Unit = {
    val env = ExecutionEnvironment.createLocalEnvironment()
    val expr1 = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1)).toTable.as('a, 'b)
    val expr2 = env.fromElements(WC("hello", 1), WC("world", 1), WC("java", 2)).toTable.as('c, 'd)
    val result = expr1.join(expr2).where("b = d").select("a, c").explain(true)
    val source = scala.io.Source.fromFile("C:\\Users\\lungao\\mygit\\flink\\flink-staging" +
      "\\flink-table\\src\\test\\testFile\\testJoin1.out").mkString
    assert(result.equals(source))
  }
  
  @Test
  def testUnionWithoutExtended() : Unit = {
    val env = ExecutionEnvironment.createLocalEnvironment()
    val expr1 = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1)).toTable
    val expr2 = env.fromElements(WC("hello", 1), WC("world", 1), WC("java", 2)).toTable
    val result = expr1.unionAll(expr2).explain()
    val source = scala.io.Source.fromFile("C:\\Users\\lungao\\mygit\\flink\\flink-staging" +
      "\\flink-table\\src\\test\\testFile\\testUnion0.out").mkString
    assert(result.equals(source))
  }
  
  @Test
  def testUnionWithExtended() : Unit = {
    val env = ExecutionEnvironment.createLocalEnvironment()
    val expr1 = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1)).toTable
    val expr2 = env.fromElements(WC("hello", 1), WC("world", 1), WC("java", 2)).toTable
    val result = expr1.unionAll(expr2).explain(true)
    val source = scala.io.Source.fromFile("C:\\Users\\lungao\\mygit\\flink\\flink-staging" +
      "\\flink-table\\src\\test\\testFile\\testUnion1.out").mkString
    assert(result.equals(source))
  }
}
