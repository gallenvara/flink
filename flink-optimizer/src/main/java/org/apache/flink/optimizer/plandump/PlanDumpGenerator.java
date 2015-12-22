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

package org.apache.flink.optimizer.plandump;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.flink.api.common.operators.CompilerHints;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.dag.BinaryUnionNode;
import org.apache.flink.optimizer.dag.DataSinkNode;
import org.apache.flink.optimizer.dag.DataSourceNode;
import org.apache.flink.optimizer.dag.OptimizerNode;
import org.apache.flink.optimizer.dag.TempMode;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.util.Utils;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.util.StringUtils;

public class PlanDumpGenerator {

	private Map<DumpableNode<?>, Integer> nodeIds; // resolves pact nodes to ids

	private int nodeCnt;

	private boolean encodeForHTML;

	private boolean extended;

	private int tabCount = 0;

	// --------------------------------------------------------------------------------------------

	public void setEncodeForHTML(boolean encodeForHTML) {
		this.encodeForHTML = encodeForHTML;
	}

	public boolean isEncodeForHTML() {
		return encodeForHTML;
	}

	public PlanDumpGenerator(boolean extended) {
		this.extended = extended;
	}


	public void dumpPactPlan(List<DataSinkNode> nodes, PrintWriter writer) {
		@SuppressWarnings("unchecked")
		List<DumpableNode<?>> n = (List<DumpableNode<?>>) (List<?>) nodes;
		compilePlan(n, writer);
	}

	public String getPactPlan(List<DataSinkNode> nodes) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		dumpPactPlan(nodes, pw);
		return sw.toString();
	}

	public String getOptimizerPlan(OptimizedPlan plan) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		dumpOptimizerPlan(plan, pw);
		pw.close();
		return sw.toString();
	}

	public void dumpOptimizerPlan(OptimizedPlan plan, PrintWriter writer) {
		Collection<SinkPlanNode> sinks = plan.getDataSinks();
		if (sinks instanceof List) {
			dumpOptimizerPlan((List<SinkPlanNode>) sinks, writer);
		} else {
			List<SinkPlanNode> n = new ArrayList<SinkPlanNode>();
			n.addAll(sinks);
			dumpOptimizerPlan(n, writer);
		}
	}

	public void dumpOptimizerPlan(List<SinkPlanNode> nodes, PrintWriter writer) {
		@SuppressWarnings("unchecked")
		List<DumpableNode<?>> n = (List<DumpableNode<?>>) (List<?>) nodes;
		compilePlan(n, writer);
	}
	
	public void printTab(int count, PrintWriter writer) {
		for (int i = 0; i < count; i++)
		writer.print("\t");
	}

	// --------------------------------------------------------------------------------------------

	private void compilePlan(List<DumpableNode<?>> nodes, PrintWriter writer) {
		// initialization to assign node ids
		this.nodeIds = new HashMap<DumpableNode<?>, Integer>();
		this.nodeCnt = 0;
		for (int i = 0; i < nodes.size(); i++) {
			visit(nodes.get(i), writer, i == 0);
		}
		
	}

	private boolean visit(DumpableNode<?> node, PrintWriter writer, boolean first) {
		// check for duplicate traversal
		if (this.nodeIds.containsKey(node)) {
			return false;
		}

		// assign an id first
		this.nodeIds.put(node, this.nodeCnt++);

		// then recurse
		for (DumpableNode<?> child : node.getPredecessors()) {
			//This is important, because when the node was already in the graph it is not allowed
			//to set first to false!
			if (visit(child, writer, first)) {
				first = false;
			}
		}
		//context format
		printTab(this.tabCount, writer);
		// check if this node should be skipped from the dump
		final OptimizerNode n = node.getOptimizerNode();

		writer.print("Stage " + this.nodeIds.get(node) + " : " + n.getOperatorName() + "\n");
		String contents;
		if (n instanceof DataSinkNode) {
			String string = n.getOperator().toString();
			int index = string.indexOf("@");
			contents = string.substring(0, index);
			
		} else if (n instanceof DataSourceNode) {
			String string  = n.getOperator().toString();
			int index = string.indexOf("(");
			contents = string.substring(0, index);
		}
		else if (n instanceof BinaryUnionNode) {
			contents = "";
		}
		else {
				contents = n.getOperator().getName();
				contents = StringUtils.showControlCharacters(contents);
				if (encodeForHTML) {
					contents = StringEscapeUtils.escapeHtml4(contents);
					contents = contents.replace("\\", "&#92;");
				}
		}

		printTab(this.tabCount + 1, writer);
		writer.print("content : " + contents + "\n");

		final PlanNode p = node.getPlanNode();
		if (p == null) {
			// finish node
			writer.print("\n");
			return true;
		}

		// output node predecessors
		Iterator<? extends DumpableConnection<?>> inConns = node.getDumpableInputs().iterator();
		String child1name = "", child2name = "";

		if (inConns != null && inConns.hasNext()) {
			// start predecessor list
			int inputNum = 0;

			while (inConns.hasNext()) {
				final DumpableConnection<?> inConn = inConns.next();
				final DumpableNode<?> source = inConn.getSource();
				if (inputNum == 0) {
					child1name += child1name.length() > 0 ? ", " : "";
					child1name += source.getOptimizerNode().getOperator().getName();
				} else if (inputNum == 1) {
					child2name += child2name.length() > 0 ? ", " : "";
					child2name = source.getOptimizerNode().getOperator().getName();
				}

				// output connection side
				if (inConns.hasNext() || inputNum > 0) {
					printTab(this.tabCount + 1, writer);
					writer.print("side : " + (inputNum == 0 ? "first" : "second") + "\n");
				}
				// output shipping strategy and channel type
				final Channel channel = (inConn instanceof Channel) ? (Channel) inConn : null;
				final ShipStrategyType shipType = channel != null ?
						channel.getShipStrategy() :
						inConn.getShipStrategy();

				String shipStrategy = null;
				if (shipType != null) {
					switch (shipType) {
						case NONE:
							// nothing
							break;
						case FORWARD:
							shipStrategy = "Forward";
							break;
						case BROADCAST:
							shipStrategy = "Broadcast";
							break;
						case PARTITION_HASH:
							shipStrategy = "Hash Partition";
							break;
						case PARTITION_RANGE:
							shipStrategy = "Range Partition";
							break;
						case PARTITION_RANDOM:
							shipStrategy = "Redistribute";
							break;
						case PARTITION_FORCED_REBALANCE:
							shipStrategy = "Rebalance";
							break;
						case PARTITION_CUSTOM:
							shipStrategy = "Custom Partition";
							break;
						default:
							throw new CompilerException("Unknown ship strategy '" + inConn.getShipStrategy().name()
									+ "' in generator.");
					}
				}

				if (channel != null && channel.getShipStrategyKeys() != null && channel.getShipStrategyKeys().size() > 0) {
					shipStrategy += " on " + (channel.getShipStrategySortOrder() == null ?
							channel.getShipStrategyKeys().toString() :
							Utils.createOrdering(channel.getShipStrategyKeys(), channel.getShipStrategySortOrder()).toString());
				}

				if (shipStrategy != null) {
					printTab(this.tabCount + 1, writer);
					writer.print("ship_strategy: " + shipStrategy + "\n");
				}

				if (channel != null) {
					String localStrategy = null;
					switch (channel.getLocalStrategy()) {
						case NONE:
							break;
						case SORT:
							localStrategy = "Sort";
							break;
						case COMBININGSORT:
							localStrategy = "Sort (combining)";
							break;
						default:
							throw new CompilerException("Unknown local strategy " + channel.getLocalStrategy().name());
					}

					if (channel != null && channel.getLocalStrategyKeys() != null && channel.getLocalStrategyKeys().size() > 0) {
						localStrategy += " on " + (channel.getLocalStrategySortOrder() == null ?
								channel.getLocalStrategyKeys().toString() :
								Utils.createOrdering(channel.getLocalStrategyKeys(), channel.getLocalStrategySortOrder()).toString());
					}

					if (localStrategy != null) {
						printTab(this.tabCount + 1, writer);
						writer.print("local_strategy : " + localStrategy + "\n");
					}

					if (channel != null && channel.getTempMode() != TempMode.NONE) {
						String tempMode = channel.getTempMode().toString();
						printTab(this.tabCount + 1, writer);
						writer.print("temp_mode : " + tempMode + "\n");
					}

					if (channel != null) {
						String exchangeMode = channel.getDataExchangeMode().toString();
						printTab(this.tabCount + 1, writer);
						writer.print("exchange_mode : " + exchangeMode + "\n");
					}
				}
				inputNum++;
			}
			// finish predecessors
		}
		
		String locString = null;
		if (p.getDriverStrategy() != null) {
			switch (p.getDriverStrategy()) {
				case NONE:
				case BINARY_NO_OP:
					break;

				case UNARY_NO_OP:
					locString = "No-Op";
					break;

				case MAP:
					locString = "Map";
					break;

				case FLAT_MAP:
					locString = "FlatMap";
					break;

				case MAP_PARTITION:
					locString = "Map Partition";
					break;

				case ALL_REDUCE:
					locString = "Reduce All";
					break;

				case ALL_GROUP_REDUCE:
				case ALL_GROUP_REDUCE_COMBINE:
					locString = "Group Reduce All";
					break;

				case SORTED_REDUCE:
					locString = "Sorted Reduce";
					break;

				case SORTED_PARTIAL_REDUCE:
					locString = "Sorted Combine/Reduce";
					break;

				case SORTED_GROUP_REDUCE:
					locString = "Sorted Group Reduce";
					break;

				case SORTED_GROUP_COMBINE:
					locString = "Sorted Combine";
					break;

				case HYBRIDHASH_BUILD_FIRST:
					locString = "Hybrid Hash (build: " + child1name + ")";
					break;
				case HYBRIDHASH_BUILD_SECOND:
					locString = "Hybrid Hash (build: " + child2name + ")";
					break;

				case HYBRIDHASH_BUILD_FIRST_CACHED:
					locString = "Hybrid Hash (CACHED) (build: " + child1name + ")";
					break;
				case HYBRIDHASH_BUILD_SECOND_CACHED:
					locString = "Hybrid Hash (CACHED) (build: " + child2name + ")";
					break;

				case NESTEDLOOP_BLOCKED_OUTER_FIRST:
					locString = "Nested Loops (Blocked Outer: " + child1name + ")";
					break;
				case NESTEDLOOP_BLOCKED_OUTER_SECOND:
					locString = "Nested Loops (Blocked Outer: " + child2name + ")";
					break;
				case NESTEDLOOP_STREAMED_OUTER_FIRST:
					locString = "Nested Loops (Streamed Outer: " + child1name + ")";
					break;
				case NESTEDLOOP_STREAMED_OUTER_SECOND:
					locString = "Nested Loops (Streamed Outer: " + child2name + ")";
					break;

				case INNER_MERGE:
					locString = "Merge";
					break;

				case CO_GROUP:
					locString = "Co-Group";
					break;

				default:
					locString = p.getDriverStrategy().name();
					break;
			}

			if (locString != null) {
				printTab(this.tabCount + 1, writer);
				writer.print("driver_strategy : ");
				writer.print(locString + "\n");
			}
		}
		//finish strategy
		
		// output node global properties
		final GlobalProperties gp = p.getGlobalProperties();
		printTab(this.tabCount + 1, writer);
		writer.print("Partitioning : " + gp.getPartitioning().name() + "\n");
		
		if (extended) {
			if (gp.getPartitioningOrdering() != null) {
				printTab(this.tabCount + 1, writer);
				writer.print("Partitioning Order : " + gp.getPartitioningOrdering().toString() + "\n");
			} else {
				printTab(this.tabCount + 1, writer);
				writer.print("Partitioning Order : none" + "\n");
			}
			if (n.getUniqueFields() == null || n.getUniqueFields().size() == 0) {
				printTab(this.tabCount + 1, writer);
				writer.print("Uniqueness : not unique" + "\n");
			} else {
				printTab(this.tabCount + 1, writer);
				writer.print("Uniqueness : " + n.getUniqueFields().toString() + "\n");
			}

			// output node size estimates
			printTab(this.tabCount + 1, writer);
			writer.print("Est. Output Size : ");
			writer.print(n.getEstimatedOutputSize() == -1 ? "unknow\n" : n.getEstimatedNumRecords() + "\n");
			printTab(this.tabCount + 1, writer);
			writer.print("Est. Cardinality : ");
			writer.print(n.getEstimatedNumRecords() == -1 ? "unknow\n" : n.getEstimatedNumRecords() + "\n");

			// output node cost
			if (p.getNodeCosts() != null) {

				printTab(this.tabCount + 1, writer);
				writer.print("Network : ");
				writer.print(p.getNodeCosts().getNetworkCost() == -1 ? "unknown\n"
						: p.getNodeCosts().getNetworkCost() + "\n");
				printTab(this.tabCount + 1, writer);
				writer.print("Disk I/O : ");
				writer.print(p.getNodeCosts().getCpuCost() == -1 ? "unknown\n"
						: (p.getNodeCosts().getCpuCost()) + "\n");
				printTab(this.tabCount + 1, writer);
				writer.print("CPU : ");
				writer.print(p.getNodeCosts().getCpuCost() == -1 ? "unknown\n"
						: (p.getNodeCosts().getCpuCost()) + "\n");
				printTab(this.tabCount + 1, writer);
				writer.print("Cumulative Network : ");
				writer.print(p.getCumulativeCosts().getNetworkCost() == -1 ? "unknown\n" : p
						.getCumulativeCosts().getNetworkCost() + "\n");
				printTab(this.tabCount + 1, writer);
				writer.print("Cumulative Disk I/O : ");
				writer.print(p.getCumulativeCosts().getDiskCost() == -1 ? "unknown\n" : p
						.getCumulativeCosts().getDiskCost() + "\n");
				printTab(this.tabCount + 1, writer);
				writer.print("Cumulative CPU : ");
				writer.print(p.getCumulativeCosts().getCpuCost() == -1 ? "unknown\n" : p
						.getCumulativeCosts().getCpuCost() + "\n");

			}

			// output the node compiler hints
			if (n.getOperator().getCompilerHints() != null) {
				CompilerHints hints = n.getOperator().getCompilerHints();
				CompilerHints defaults = new CompilerHints();

				String size = hints.getOutputSize() == defaults.getOutputSize() ? "none" : String.valueOf(hints.getOutputSize());
				String card = hints.getOutputCardinality() == defaults.getOutputCardinality() ? "none" : String.valueOf(hints.getOutputCardinality());
				String width = hints.getAvgOutputRecordSize() == defaults.getAvgOutputRecordSize() ? "none" : String.valueOf(hints.getAvgOutputRecordSize());
				String filter = hints.getFilterFactor() == defaults.getFilterFactor() ? "none" : String.valueOf(hints.getFilterFactor());

				printTab(this.tabCount + 1, writer);
				writer.print("Output Size (bytes) : " + size + "\n");
				printTab(this.tabCount + 1, writer);
				writer.print("Output Cardinality : " + card + "\n");
				printTab(this.tabCount + 1, writer);
				writer.print("Avg. Output Record Size (bytes) : " + width + "\n");
				printTab(this.tabCount + 1, writer);
				writer.print("Filter Factor : " + filter + "\n");
			}
		}
		// finish node
		writer.print("\n");
		this.tabCount++;
		return true;
	}
}
