package org.apache.flink.api.java.sampling;

import java.util.Iterator;

/**
 * Created by lungao on 8/25/2015.
 */

public class ScalableSRSTest {
	public static void main(String[] args){
		ReservoirSamplerWithoutReplacement<Double> rswr = new ReservoirSamplerWithoutReplacement<Double>(100);
		Iterator<Double> input = null;
		Iterator<IntermediateSampleData<Double>> iterator = rswr.sampleInPartition(input);
	}
}
