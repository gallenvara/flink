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

import java.io.IOException;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.base.StringComparator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.types.StringValue;

@SuppressWarnings("rawtypes")
public class IntStringPairComparator extends TypeComparator<IntStringPair> {

	private static final long serialVersionUID = 1L;

	private String reference;
	
	private boolean acending;

	private final TypeComparator[] comparators = new TypeComparator[] {new StringComparator(acending)};
	
	public IntStringPairComparator(){}
	
	public IntStringPairComparator(boolean acending){
		this.acending = acending;
	}

	@Override
	public int hash(IntStringPair record) {
		return record.getValue().hashCode();
	}

	@Override
	public void setReference(IntStringPair toCompare) {
		this.reference = toCompare.getValue();
	}

	@Override
	public boolean equalToReference(IntStringPair candidate) {
		return this.reference.equals(candidate.getValue());
	}

	@Override
	public int compareToReference(TypeComparator<IntStringPair> referencedComparator) {
		return this.reference.compareTo(((IntStringPairComparator) referencedComparator).reference);
	}

	@Override
	public int compare(IntStringPair first, IntStringPair second) {
		int flag = first.getValue().compareTo(second.getValue());
		return acending ? flag : -flag;
	}

	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource)
			throws IOException {
		int flag =  StringValue.readString(firstSource).compareTo(StringValue.readString(secondSource));
		return acending ? flag : -flag;
	}

	@Override
	public boolean supportsNormalizedKey() {
		return false;
	}

	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}

	@Override
	public int getNormalizeKeyLen() {
		return Integer.MAX_VALUE;
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return false;
	}

	@Override
	public void putNormalizedKey(IntStringPair record, MemorySegment target,
								 int offset, int numBytes) {
		throw new RuntimeException("not implemented");
	}

	@Override
	public void writeWithKeyNormalization(IntStringPair record,
										  DataOutputView target) throws IOException {
		throw new RuntimeException("not implemented");
	}

	@Override
	public IntStringPair readWithKeyDenormalization(IntStringPair record,
												 DataInputView source) throws IOException {
		throw new RuntimeException("not implemented");
	}

	@Override
	public boolean invertNormalizedKey() {
		return false;
	}

	@Override
	public TypeComparator<IntStringPair> duplicate() {
		return new IntStringPairComparator();
	}

	@Override
	public int extractKeys(Object record, Object[] target, int index) {
		target[index] = ((IntStringPair) record).getValue();
		return 1;
	}

	@Override public TypeComparator[] getFlatComparators() {
		return comparators;
	}
}

