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
package org.apache.flink.runtime.operators.testutils.types;

public class IntStringPair implements Comparable<IntStringPair>{
	
	private int key;
	private String value;
	
	public IntStringPair(){}
	
	public IntStringPair(int key, String value) {
		this.key = key;
		this.value = value;
	}
	
	public int getKey(){
		return this.key;
	}
	
	public void setKey(int key) {
		this.key = key;
	}
	
	public String getValue(){
		return this.value;
	}
	
	public void setValue(String value) {
		this.value = value;
	}
	
	@Override
	public String toString() {
		return "(" + this.key + "/" + this.value + ")";
	}

	@Override
	public int compareTo(IntStringPair o) {
		return this.getValue().compareTo(o.getValue()) > 0 ? 1 : -1;
	}
}
