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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.typeinfo.RowTypeInfo
import org.junit.Test

import scala.reflect.ClassTag

class RowTest {

  @Test
  def testCsv() : Unit = {
    val env = ExecutionEnvironment.createLocalEnvironment()
    val filePath = "../testCsv.csv"
    val typeInfo = new RowTypeInfo(Seq(BasicTypeInfo.STRING_TYPE_INFO, 
      BasicTypeInfo.INT_TYPE_INFO), Seq("word", "number"))
    val source = env.readCsvFile(filePath)(ClassTag(classOf[Row]), typeInfo)
    println(source.collect())
  }
}
