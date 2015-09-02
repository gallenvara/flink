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
package org.apache.flink.api.java.sampling;

import com.google.common.base.Preconditions;
import com.sun.javafx.scene.control.skin.VirtualFlow;

import java.util.*;

/**
 * A simple in memory implementation of Reservoir Sampling without replacement, and with only one
 * pass through the input iteration whose size is unpredictable. The basic idea behind this sampler
 * implementation is to generate a random number for each input element as its weight, select the
 * top K elements with max weight. As the weights are generated randomly, so are the selected
 * top K elements. The algorithm is implemented using the {@link DistributedRandomSampler}
 * interface. In the first phase, we generate random numbers as the weights for each element and
 * select top K elements as the output of each partitions. In the second phase, we select top K
 * elements from all the outputs of the first phase.
 *
 * This implementation refers to the algorithm described in <a href="researcher.ibm.com/files/us-dpwoodru/tw11.pdf">
 * "Optimal Random Sampling from Distributed Streams Revisited"</a>.
 *
 * @param <T> The type of the sampler.
 */
public class ReservoirSamplerWithoutReplacement<T> extends DistributedRandomSampler<T> {

	private final Random random;
	private final static int SOURCE_SIZE = 10000000;

	/**
	 * Create a new sampler with reservoir size and a supplied random number generator.
	 *
	 * @param numSamples Maximum number of samples to retain in reservoir, must be non-negative.
	 * @param random     Instance of random number generator for sampling.
	 */
	public ReservoirSamplerWithoutReplacement(int numSamples, Random random) {
		super(numSamples);
		Preconditions.checkArgument(numSamples >= 0, "numSamples should be non-negative.");
		this.random = random;
	}

	/**
	 * Create a new sampler with reservoir size and a default random number generator.
	 *
	 * @param numSamples Maximum number of samples to retain in reservoir, must be non-negative.
	 */
	public ReservoirSamplerWithoutReplacement(int numSamples) {
		this(numSamples, new Random());
	}

	/**
	 * Create a new sampler with reservoir size and the seed for random number generator.
	 *
	 * @param numSamples Maximum number of samples to retain in reservoir, must be non-negative.
	 * @param seed       Random number generator seed.
	 */
	public ReservoirSamplerWithoutReplacement(int numSamples, long seed) {

		this(numSamples, new Random(seed));
	}

	public double min(double x, double y){
		return x < y ? x : y;
	}

	public double max(double x, double y){
		return x > y ? x : y;
	}

	public double threshold_q1(int index){
		double expectSuccessRate = 0.9;//
		double y = - Math.log(1 - expectSuccessRate) / index;
		return min(1.0, numSamples*1.0 /index + y + Math.sqrt(y*y + 2*y*numSamples/index));
	}


	@Override
	public Iterator<IntermediateSampleData<T>> sampleInPartition(Iterator<T> input) {
		if (numSamples == 0) {
			return EMPTY_INTERMEDIATE_ITERABLE;
		}

		/**List<IntermediateSampleData<T>> selectList = new ArrayList<IntermediateSampleData<T>>();
		int index = 0;

		while (input.hasNext()) {
			T element = input.next();
			index++;
			double rand = random.nextDouble();
			double q1 = threshold_q1(index);
			if (rand < q1) {
				System.out.println(index + "is added to the queue ,now queue.size() is " + selectList.size());
				selectList.add(new IntermediateSampleData<T>(rand, element));
			}
			}
		System.out.println(selectList.size());
		if (numSamples > selectList.size()) {
			System.out.println("Sampling failure");
		}
		else if (numSamples < selectList.size()) {
			Collections.sort(selectList);
			int num = selectList.size();
			while (num > numSamples){
				selectList.remove(num-1);
				num--;
			}
		}*/
		PriorityQueue<IntermediateSampleData<T>> queue = new PriorityQueue<IntermediateSampleData<T>>();
		List<IntermediateSampleData<T>> selectList = new ArrayList<IntermediateSampleData<T>>();
		int index = 0;
		while (input.hasNext()) {
			T element = input.next();
			index++;
			double rand = random.nextDouble();
			double q1 = threshold_q1(index);
			//System.out.println(queue.size());
			if (rand < q1) {
				//System.out.println(index + "is added to the queue ,now queue.size() is " + queue.size());
				queue.add(new IntermediateSampleData<T>(rand, element));

			}
		}
		System.out.println("Samples= " + numSamples +" , reject element = " + (SOURCE_SIZE - numSamples));
		IntermediateSampleData<T> smallest = queue.peek();

		if (numSamples >= queue.size()) {
			System.out.println("Sampling Failure");
			return queue.iterator();
		}
		else if (numSamples < queue.size()) {
			int num = 0;
			while (num < numSamples){

				selectList.add(smallest);
				queue.remove(smallest);
				smallest = queue.peek();
				num++;
			}
		}
		return selectList.iterator();
	}
}
