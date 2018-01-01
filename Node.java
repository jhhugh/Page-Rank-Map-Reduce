/*Jimmy Hughes
 * Parallel & Distributed Computing
 * Page Rank
 * 
 */

package pageRank;

import java.io.*;
import java.util.*;
 
import org.apache.commons.lang.StringUtils;
 
// Node class sets up the nodes of the adjacency list
public class Node {
  private double pageRank = 0.25;
  private String[] adjacencyList;
 
  public static final char fieldSeparator = '\t';
 
  public double getPageRank() {
    return pageRank;
  }
 
  public Node setPageRank(double pageRank) {
    this.pageRank = pageRank;
    return this;
  }
 
  public String[] getAdjacencyList() {
    return adjacencyList;
  }
 
  public Node setAdjacencyList(String[] adjacencyList) {
    this.adjacencyList = adjacencyList;
    return this;
  }
 
  public boolean containsAdjacentNodes() {
    return adjacencyList != null;
  }
 
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(pageRank);
 
    if (getAdjacencyList() != null) {
      sb.append(fieldSeparator)
          .append(StringUtils
              .join(getAdjacencyList(), fieldSeparator));
    }
    return sb.toString();
  }
 
  /*Updates the adjacency list of a node before the start of every map and reduce task
   * using values from the previous iteration
   */
  public static Node afterMR(String value) throws IOException {
    String[] parts = StringUtils.splitPreserveAllTokens(
        value, fieldSeparator);
    if (parts.length < 1) {
      throw new IOException(
          "Expected 1 or more parts but received " + parts.length);
    }
    Node node = new Node()
        .setPageRank(Double.valueOf(parts[0]));
    if (parts.length > 1) {
      node.setAdjacencyList(Arrays.copyOfRange(parts, 1,
          parts.length));
    }
    return node;
  }
}
