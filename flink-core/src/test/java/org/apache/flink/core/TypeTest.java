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

package org.apache.flink.core;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.MissingTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.junit.Assert;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Alipay.com Inc
 * Copyright (c) 2004-2017 All Rights Reserved.
 *
 * @author gaolun on 18/3/26.
 */
public class TypeTest {

	public static void main(String[] args) {
		//Param<String, Integer> param = new Param<>();
		MapFunction<Tuple2<Tuple1<Integer>, Boolean>, Tuple2<Tuple1<Integer>, String>> f = (i) -> null;

		TypeInformation<?> typeInformation = TypeExtractor.getMapReturnTypes(f, TypeInfoParser.parse("Tuple2<Tuple1<Integer>, Boolean>"));
		if (!(typeInformation instanceof MissingTypeInfo)) {
			Assert.assertTrue(typeInformation.isTupleType());
			Assert.assertEquals(2, typeInformation.getArity());
			Assert.assertTrue(((TupleTypeInfo<?>) typeInformation).getTypeAt(0).isTupleType());
//			Assert.assertEquals(((TupleTypeInfo<?>)((TupleTypeInfo<?>) typeInformation).getTypeAt(0)).getFieldNames()[0], "");
			Assert.assertEquals(((TupleTypeInfo<?>) typeInformation).getTypeAt(1), BasicTypeInfo.STRING_TYPE_INFO);
		}

	}
}
class ParameterizedTypeImpl implements ParameterizedType {

	@Override
	public Type[] getActualTypeArguments() {
		return new Type[0];
	}

	@Override
	public Type getRawType() {
		return null;
	}

	@Override
	public Type getOwnerType() {
		System.out.println();
		return null;
	}
}

class Param<T1, T2> {

	class A {}
	class B extends A {}

	private Class<T1> entityClass;
	public Param (){
		Type type = getClass().getGenericSuperclass();
		System.out.println("getClass() == " + getClass());
		System.out.println("type = " + type);
		Type trueType = ((ParameterizedType)type).getActualTypeArguments()[0];
		System.out.println("trueType1 = " + trueType);
		trueType = ((ParameterizedType)type).getActualTypeArguments()[1];
		System.out.println("trueType2 = " + trueType);
		this.entityClass = (Class<T1>)trueType;
		System.out.println("entityClass = " + entityClass);

		B t = new B();
		type = t.getClass().getGenericSuperclass();

		System.out.println("B is A's super class :" + ((ParameterizedType)type).getActualTypeArguments().length);
	}

}
