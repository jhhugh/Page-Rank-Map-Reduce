/*Jimmy Hughes
 * Parallel & Distributed Computing
 * Homework 3
 * Page Rank
 * 
 */

package pageRank;

import java.io.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
/*
 * Reducer sums the pageranks and initializes them to the nodes. Emits key value pair
 */
public class Reducer extends Reducer<Text, Text, Text, Text> {
 
	public static final double CONVERGENCE_SCALING_FACTOR = 1000.0;
	public static final double DAMPING_FACTOR = 0.85; //Given in initial pagerank equation
	public static String CONF_NUM_NODES_GRAPH = "pagerank.numnodes";
	private int numberOfNodes;
	
	public static enum Counter {
	C_DELTA
	}
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		numberOfNodes = context.getConfiguration().getInt(CONF_NUM_NODES_GRAPH, 0);
	}
	
	private Text outValue = new Text();
	
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
	
		
		double summedPageRanks = 0;
		Node originalNode = new Node();
		
		//for all values of a given key. 
		for (Text textValue : values) {
		
		  Node node = Node.fromMR(textValue.toString());
		
		  //Store node as a variable else it is a pagerank and will be added to the sum.
		  if (node.containsAdjacentNodes()) {

		    originalNode = node;
		  } else {
		    summedPageRanks += node.getPageRank();
		  }
		}
		
		double dampingFactor = ((1.0 - DAMPING_FACTOR) / (double) numberOfNodesInGraph);
		
		double newPageRank = dampingFactor + (DAMPING_FACTOR * summedPageRanks);// new page rank with consideration of new damping factor
		
		double delta = originalNode.getPageRank() - newPageRank;
		
		originalNode.setPageRank(newPageRank);
		
		outValue.set(originalNode.toString());
		
		context.write(key, outValue);
		int scaledDelta = Math.abs((int) (delta * CONVERGENCE_SCALING_FACTOR));
		
		context.getCounter(Counter.CONV_DELTAS).increment(scaledDelta);//scaled delta set as counter
	}
}
