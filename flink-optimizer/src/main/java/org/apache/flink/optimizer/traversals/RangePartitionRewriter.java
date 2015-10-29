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
package org.apache.flink.optimizer.traversals;

import org.apache.flink.api.common.distributions.CommonRangeBoundaries;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase;
import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.common.operators.base.MapPartitionOperatorBase;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.java.functions.AssignRangeIndex;
import org.apache.flink.api.java.functions.PartitionIDRemoveWrapper;
import org.apache.flink.api.java.functions.RangeBoundaryBuilder;
import org.apache.flink.api.java.functions.SampleInCoordinator;
import org.apache.flink.api.java.functions.SampleInPartition;
import org.apache.flink.api.java.sampling.IntermediateSampleData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.optimizer.dag.GroupReduceNode;
import org.apache.flink.optimizer.dag.MapNode;
import org.apache.flink.optimizer.dag.MapPartitionNode;
import org.apache.flink.optimizer.dag.TempMode;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.NamedChannel;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.util.Utils;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.util.Visitor;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class RangePartitionRewriter implements Visitor<PlanNode> {

	OptimizedPlan plan;

	public RangePartitionRewriter(OptimizedPlan plan) {
		this.plan = plan;
	}

	@Override
	public boolean preVisit(PlanNode visitable) {
		return true;
	}

	@Override
	public void postVisit(PlanNode visitable) {
		List<Channel> outgoingChannels = visitable.getOutgoingChannels();
		List<Channel> appendOutgoingChannels = new LinkedList<>();
		List<Channel> removeOutgoingChannels = new LinkedList<>();
		for (Channel channel : outgoingChannels) {
			ShipStrategyType shipStrategy = channel.getShipStrategy();
			if (shipStrategy == ShipStrategyType.PARTITION_RANGE) {
				if (channel.getDataDistribution() == null) {
					removeOutgoingChannels.add(channel);
					appendOutgoingChannels.addAll(rewriteRangePartitionChannel(channel));
				}
			}
		}
		outgoingChannels.addAll(appendOutgoingChannels);
		for (Channel channel : removeOutgoingChannels) {
			outgoingChannels.remove(channel);
		}
	}

	private List<Channel> rewriteRangePartitionChannel(Channel channel) {
		List<Channel> appendOutgoingChannels = new LinkedList<>();
		PlanNode sourceNode = channel.getSource();
		PlanNode targetNode = channel.getTarget();
		int sourceParallelism = sourceNode.getParallelism();
		int targetParallelism = targetNode.getParallelism();
		TypeComparatorFactory<?> comparator = Utils.getShipComparator(channel, this.plan.getOriginalPlan().getExecutionConfig());
		// 1. Fixed size sample in each partitions.
		long seed = org.apache.flink.api.java.Utils.RNG.nextLong();
		int sampleSize = 20 * targetParallelism;
		SampleInPartition sampleInPartition = new SampleInPartition(false, sampleSize, seed);
		TypeInformation<?> sourceOutputType = sourceNode.getOptimizerNode().getOperator().getOperatorInfo().getOutputType();
		TypeInformation<IntermediateSampleData> isdTypeInformation = TypeExtractor.getForClass(IntermediateSampleData.class);
		UnaryOperatorInformation sipOperatorInformation = new UnaryOperatorInformation(sourceOutputType, isdTypeInformation);
		MapPartitionOperatorBase sipOperatorBase = new MapPartitionOperatorBase(sampleInPartition, sipOperatorInformation, "Sample in partitions");
		MapPartitionNode sipNode = new MapPartitionNode(sipOperatorBase);
		Channel sipChannel = new Channel(sourceNode, TempMode.NONE);
		sipChannel.setShipStrategy(ShipStrategyType.FORWARD, channel.getDataExchangeMode());
		SingleInputPlanNode sipPlanNode = new SingleInputPlanNode(sipNode, "SampleInPartition PlanNode", sipChannel, DriverStrategy.MAP_PARTITION);
		sipPlanNode.setParallelism(sourceParallelism);
		sipChannel.setTarget(sipPlanNode);
		appendOutgoingChannels.add(sipChannel);
		this.plan.getAllNodes().add(sipPlanNode);

		// 2. Fixed size sample in a single coordinator.
		SampleInCoordinator sampleInCoordinator = new SampleInCoordinator(false, sampleSize, seed);
		UnaryOperatorInformation sicOperatorInformation = new UnaryOperatorInformation(isdTypeInformation, sourceOutputType);
		GroupReduceOperatorBase sicOperatorBase = new GroupReduceOperatorBase(sampleInCoordinator, sicOperatorInformation, "Sample in coordinator");
		GroupReduceNode sicNode = new GroupReduceNode(sicOperatorBase);
		Channel sicChannel = new Channel(sipPlanNode, TempMode.NONE);
		sicChannel.setShipStrategy(ShipStrategyType.PARTITION_HASH, channel.getShipStrategyKeys(), channel.getShipStrategySortOrder(), null, channel.getDataExchangeMode());
		SingleInputPlanNode sicPlanNode = new SingleInputPlanNode(sicNode, "SampleInCoordinator PlanNode", sicChannel, DriverStrategy.ALL_GROUP_REDUCE);
		sicPlanNode.setParallelism(1);
		sicChannel.setTarget(sicPlanNode);
		sipPlanNode.addOutgoingChannel(sicChannel);
		this.plan.getAllNodes().add(sicPlanNode);

		// 3. Use sampled data to build range boundaries.
		RangeBoundaryBuilder rangeBoundaryBuilder = new RangeBoundaryBuilder(comparator, targetParallelism);
		TypeInformation<CommonRangeBoundaries> rbTypeInformation = TypeExtractor.getForClass(CommonRangeBoundaries.class);
		UnaryOperatorInformation rbOperatorInformation = new UnaryOperatorInformation(sourceOutputType, rbTypeInformation);
		MapPartitionOperatorBase rbOperatorBase = new MapPartitionOperatorBase(rangeBoundaryBuilder, rbOperatorInformation, "RangeBoundaryBuilder");
		MapPartitionNode rbNode= new MapPartitionNode(rbOperatorBase);
		Channel rbChannel = new Channel(sicPlanNode, TempMode.NONE);
		rbChannel.setShipStrategy(ShipStrategyType.FORWARD, channel.getDataExchangeMode());
		SingleInputPlanNode rbPlanNode = new SingleInputPlanNode(rbNode, "RangeBoundary PlanNode", rbChannel, DriverStrategy.MAP_PARTITION);
		rbPlanNode.setParallelism(1);
		rbChannel.setTarget(rbPlanNode);
		sicPlanNode.addOutgoingChannel(rbChannel);
		this.plan.getAllNodes().add(rbPlanNode);

		// 4. Take range boundaries as broadcast input and take the tuple of partition id and record as output.
		AssignRangeIndex assignRangeIndex = new AssignRangeIndex(comparator);
		TypeInformation<Tuple2> ariOutputTypeInformation = new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO, sourceOutputType);
		UnaryOperatorInformation ariOperatorInformation = new UnaryOperatorInformation(sourceOutputType, ariOutputTypeInformation);
		MapPartitionOperatorBase ariOperatorBase = new MapPartitionOperatorBase(assignRangeIndex, ariOperatorInformation, "Assign Range Index");
		MapPartitionNode ariNode= new MapPartitionNode(ariOperatorBase);
		Channel ariChannel = new Channel(sourceNode, TempMode.NONE);
		ariChannel.setShipStrategy(ShipStrategyType.FORWARD, channel.getDataExchangeMode());
		SingleInputPlanNode ariPlanNode = new SingleInputPlanNode(ariNode, "AssignRangeIndex PlanNode", ariChannel, DriverStrategy.MAP_PARTITION);
		ariPlanNode.setParallelism(sourceParallelism);
		ariChannel.setTarget(ariPlanNode);
		appendOutgoingChannels.add(ariChannel);
		this.plan.getAllNodes().add(ariPlanNode);

		channel.setSource(ariPlanNode);
		ariPlanNode.addOutgoingChannel(channel);

		NamedChannel broadcastChannel = new NamedChannel("RangeBoundaries", rbPlanNode);
		broadcastChannel.setShipStrategy(ShipStrategyType.BROADCAST, channel.getDataExchangeMode());
		broadcastChannel.setTarget(ariPlanNode);
		List<NamedChannel> broadcastChannels = new ArrayList<>(1);
		broadcastChannels.add(broadcastChannel);
		ariPlanNode.setBroadcastInputs(broadcastChannels);

		// 5. Remove the partition id.
		PartitionIDRemoveWrapper partitionIDRemoveWrapper = new PartitionIDRemoveWrapper();
		UnaryOperatorInformation prOperatorInformation = new UnaryOperatorInformation(ariOutputTypeInformation, sourceOutputType);
		MapOperatorBase prOperatorBase = new MapOperatorBase(partitionIDRemoveWrapper, prOperatorInformation, "PartitionID Remover");
		MapNode prRemoverNode = new MapNode(prOperatorBase);
		Channel prChannel = new Channel(targetNode, TempMode.NONE);
		prChannel.setShipStrategy(ShipStrategyType.FORWARD, channel.getDataExchangeMode());
		SingleInputPlanNode prPlanNode = new SingleInputPlanNode(prRemoverNode, "PartitionIDRemover", prChannel, DriverStrategy.MAP);
		prChannel.setTarget(prPlanNode);
		prPlanNode.setParallelism(targetParallelism);
		this.plan.getAllNodes().add(prPlanNode);

		List<Channel> outgoingChannels = targetNode.getOutgoingChannels();
		List<Channel> backupOutgoingChannels = new LinkedList<>();
		backupOutgoingChannels.addAll(outgoingChannels);
		outgoingChannels.clear();
		Channel partitionChannel = new Channel(targetNode, TempMode.NONE);
		partitionChannel.setTarget(prPlanNode);
		outgoingChannels.add(partitionChannel);
		for (Channel outgoingChannel : backupOutgoingChannels) {
			outgoingChannel.setSource(prPlanNode);
			prPlanNode.addOutgoingChannel(outgoingChannel);
		}
		return appendOutgoingChannels;
	}
}
