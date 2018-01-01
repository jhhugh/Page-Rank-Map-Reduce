/*Jimmy Hughes
 * Parallel & Distributed Computing
 * Page Rank
 * 
 *This program takes an input file (graph of data) and its adjacency list and generates a file that contains
 *the nodes, their page rank and adjacency list
 * 
 */

package pageRank;

import java.io.*;
import java.util.*;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 

/*
 * PageRank class initializes page rank value and reads input file and creates an input file to be used in the first iteration of the
 * mapper. Output files created by reducer.
 */
public final class PageRank {
 
	  public static void main(String[]args) throws Exception {
 
	    String inputFile = args[0];
	    String outputDir = args[1];
 
	    iterativeMapReduce(inputFile, outputDir);
	  }
 
	  public static void iterativeMapReduce(String input, String output) throws Exception {
 
	    Configuration conf = new Configuration();
	    
	    Path outputPath = new Path(output);
	    outputPath.getFileSystem(conf).delete(outputPath, true);
	    outputPath.getFileSystem(conf).mkdirs(outputPath);
 
	    Path inputPath = new Path(outputPath, "input.txt");
 
	    int numNodes = createInputFile(new Path(input), inputPath);
 
	    int iter = 1;
	    double desiredConvergence = 0.005; //desired convergence
 
	    // iterate MapReduce until desired convergence is reached.
	    while (true) {
 
	      Path jobOutputPath = new Path(outputPath, String.valueOf(iter));
 
	      System.out.println("======================================");
	      System.out.println("=  Iteration:    " + iter);
	      System.out.println("=  Input:   " + inputPath);
	      System.out.println("=  Output:  " + jobOutputPath);
	      System.out.println("======================================");
 
	      if (calcPageRank(inputPath, jobOutputPath, numNodes) < desiredConvergence) {
	    	  System.out.println("Convergence is below " + desiredConvergence + ". Complete!");
	    	  break;
	      }
	      inputPath = jobOutputPath;
	      iter++;
	    }
	  }
 
	  public static int createInputFile(Path file, Path targetFile)
	      throws IOException {
	    Configuration conf = new Configuration();
	    
	    FileSystem fs = file.getFileSystem(conf);
 
	    int numNodes = getNumNodes(file);
	    double initialPageRank = 1.0 / (double) numNodes;
 
	    OutputStream os = fs.create(targetFile);
	    LineIterator iter = IOUtils.lineIterator(fs.open(file), "UTF8");
 
	    //Iterate through the file write to another file with pageranks and nodeid
	    while (iter.hasNext()) {
	      String line = iter.nextLine();
 
	      String[] parts = StringUtils.split(line);
 
	      Node node = new Node()
	          .setPageRank(initialPageRank)
	          .setAdjacencyList(
	              Arrays.copyOfRange(parts, 1, parts.length));
	      IOUtils.write(parts[0] + '\t' + node.toString() + '\n', os);
	    }
	    os.close();
	    return numNodes;
	  }
 
	  public static int getNumNodes(Path file) throws IOException {
		  
	    Configuration conf = new Configuration();  
	    FileSystem fs = file.getFileSystem(conf);
 
	    return IOUtils.readLines(fs.open(file), "UTF8").size();
	  }
 
	  public static double calcPageRank(Path inputPath, Path outputPath, int numNodes)
	      throws Exception {
	    Configuration conf = new Configuration();
	    
	    conf.setInt(Reduce.CONF_NUM_NODES_GRAPH, numNodes); // Allow number in the graph in configuration to be accessed by reducer
 
	    Job job = Job.getInstance(conf, "PageRankJob");
	    job.setJarByClass(PageRank.class);
	    job.setMapperClass(Mapper.class);
	    job.setReducerClass(Reducer.class);
 
	    job.setInputFormatClass(KeyValueTextInputFormat.class);
 
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
 
	    FileInputFormat.setInputPaths(job, inputPath);
	    FileOutputFormat.setOutputPath(job, outputPath);
 
	    if (!job.waitForCompletion(true)) {
	      throw new Exception("Error");
	    }
 
	    //Summed convergence values from reducer
	    long summedConvergence = job.getCounters().findCounter(
	        Reduce.Counter.CONV_DELTAS).getValue();
	    
	    //Calc convergence based on sum of convergence, conv scaling factor and number of nodes
	    double convergence =
	        ((double) summedConvergence /
	            Reduce.CONVERGENCE_SCALING_FACTOR) /
	            (double) numNodes;
 
	    System.out.println("======================================");
	    System.out.println("=  Num nodes:           " + numNodes);
	    System.out.println("=  Summed convergence:  " + summedConvergence);
	    System.out.println("=  Convergence:         " + convergence);
	    System.out.println("======================================");
 
	    return convergence;
	  }
 
 
	}
