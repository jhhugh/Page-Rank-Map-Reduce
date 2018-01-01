/*Jimmy Hughes
 * Parallel & Distributed Computing
 * Homework 3
 * Page Rank
 * 
 */

package pageRank;

import java.io.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
/*
 * Map Calculates pagerank for each node. Emits key value pair for each node.
 */
public class Map extends Mapper<Text, Text, Text, Text> {
	
		private Text outKey = new Text();
		private Text outValue  = new Text();
	
	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		
			context.write(key, value);// emit(nodeid,n)
			
			Node node = Node.fromMR(value.toString()); //all nodes in the adjacency list
			
			if(node.getAdjacencyList() != null && node.getAdjacencyList().length > 0) {
				
			  // Calculate pagerank by dividing the nodes pagerank by adjacent ongoing nodes	
			  double outboundPageRank = node.getPageRank() /(double)node.getAdjacencyList().length;
			  
			  // go through all the nodes and propagate PageRank to them 
				  for (int i = 0; i < node.getAdjacencyList().length; i++) {
					  
				    String neighbor = node.getAdjacencyList()[i];
				    outKey.set(neighbor);
				    Node adjacentNode = new Node().setPageRank(outboundPageRank);
				    
				    outValue.set(adjacentNode.toString());
				    context.write(outKey, outValue);//emit(nodeid, pagerank)
				  }
			}
		}
	}

