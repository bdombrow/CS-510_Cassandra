import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * SCC.java
 * 
 * This program reads a list of edges in from a database, constructs a graph 
 * from these edges, and then uses Trajan's algorithm for detecting strongly-
 * connected components to print out each SCC found.  
 * 
 * It does so in two steps:
 * 1) All SCCs are printed out, even singletons with edges to themselves.
 * 2) All SCCs are printed out, but singletons are omitted.
 * 
 * @author Josh Dorothy
 *
 */
public class BridgesTarjan {

	/**
	 * Graph
	 * 
	 * The Graph object is a simple data structure serving as a way to keep
	 * a global tally of existing nodes and edges.  Any new node should be
	 * added to the graph, and the graph should be queried for the existence
	 * of a node before creating a new one.
	 * 
	 * Nodes are stored in a hashmap - the integer "name" of the node serves
	 * as the key, and the node object itself is the value.
	 */
	static class Graph {

		static HashMap<Integer, Node> nodeList;		
		
		public Graph() {						
			nodeList = new HashMap<Integer, Node>();
		}
		
		public void addNode (Node n) {			
			nodeList.put(n.name, n);
		}

		public boolean hasNode (int n) {			
			return (nodeList.containsKey(n));
		}
		
		public Node getNode (int n) {			
			return nodeList.get(n);
		}
		
		public Set<Integer> nodeList () {
			return nodeList.keySet();
		}
	}
	
	/**
	 * Node
	 * 
	 * The Node object is a simple data structure serving as convenient storage
	 * for the various values required by Tarjan's algorithm.  It also contains
	 * methods to add a new edge and check for the existence of an edge.
	 * 
	 * The edges are stored in a simple linked list.
	 *
	 */
	static class Node {

		int lowlink;
		int index;
		int name;
		LinkedList<Integer> edgeList;		
		
		// Entry node
		public Node (int node) {
			lowlink = -1;
			index = -1;
			name = node;
			edgeList = new LinkedList<Integer>();			
		}
				
		public boolean hasEdge (int edge) {
			return edgeList.contains(edge);
		}
		
		public void addEdge (int edge) {
			edgeList.add(edge);
		}
	}
	
	// Global variables and data structures for a more convenient
	// implementation of Tarjan's algorithm:
	
	static Graph graph = new Graph();
	static Stack<Node> stack = new Stack<Node>();
	static int index;
	static int numSCCs;
	static LinkedList<LinkedList<Integer>> SCClist;
	final static String COLUMN_FAMILY = "Edges";
	
	/**
	 * main() serves as the driver to doTarjan(), which is the actual algorithm
	 * implementation.  Main() processes the database input, constructs the
	 * graph, and makes the call to doTarjan().
	 */
	public static void main (String args[]) {
		
		int entryNode = 0;
		numSCCs = 1;
		SCClist = new LinkedList<LinkedList<Integer>>();

		String temp = "";

		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		
		System.out.print("Please enter the entry node to search from: ");
		try {
			temp = reader.readLine();
			entryNode = Integer.parseInt(temp);
			graph.addNode(new Node(entryNode));			
		} catch (Exception e) {
			System.err.println("There was a problem parsing the entry node.");
			System.exit(1);
		}

		// Open up a connection to the database using the DBHelper object.
		DBHelper db = null;
		List<String> edgeList = null;
		
		// Query for all edges in the graph.
		try {
			db = new DBHelper();
			edgeList = db.getEdges();
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}
	
		// Loop until there are no more query results to process.
		
		System.out.println();
		System.out.println("Found the following edges in the database: ");
		System.out.println("--------------------------------------------");
		while (!edgeList.isEmpty()) {
			try {
				
				// Each edge is represented as a-b in the database:
				//   a = the "name" of the node
				//   b = a node that a connects to				
				System.out.println(edgeList.get(0));
				String[] edge = edgeList.remove(0).split("-");
				
				int node1 = Integer.parseInt(edge[0]);
				int node2 = Integer.parseInt(edge[1]);
		
				// The first node already exists:
				
				if (graph.hasNode(node1)) {
					
					Node n = graph.getNode(node1);
					
					// The first node already has an edge to the second node:
					
					if (n.hasEdge(node2))
						System.out.println("That directed edge already exists.");
					
					// The first node does not have an edge to the second node:
					
					else {						
						n.addEdge(node2);
						
						// If there is an edge to the second node, but the
						// second node doesn't exist yet, create it as well.
						
						if (!graph.hasNode(node2))
							graph.addNode(new Node(node2));
					}
				}
				
				// The first node does not exist:
				
				else {
					Node n = new Node(node1);
					n.addEdge(node2);
					graph.addNode(n);
					graph.addNode(new Node(node2));
				}				
			}
			catch (Exception e) {
				System.err.println("There was a problem parsing this node. Please try again.");
				System.err.println(e);
			}
		}
		
		// Close the database now that we're finished.
		try {
			db.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println();
		
		// **********************************
		// Pass 1:  Normal Tarjan's Algorithm
		// **********************************
		
		System.out.println("Tarjan's SCC Algorithm: ");
		System.out.println("****************************************");
		doTarjan(true);
		printBridges();
		SCClist.clear();
		System.out.println();		
		
		// **********************************************************
		// Pass 2:  Modified Tarjan's Algorithm (removing singletons)
		// **********************************************************

		resetNodes();
		numSCCs = 1;
		System.out.println();
		System.out.println("Tarjan's SCC Algorithm, no singletons: ");
		System.out.println("****************************************");		
		doTarjan(false);
		printBridges();
		SCClist.clear();		
	}
	
	/**
	 * printBridges() uses the list of SCCs collected by doTarjan() to print
	 * out any bridge edges resulting from that SCC.  It does so by iterating
	 * through each node in the SCC, and checking that node's edges to see if
	 * any of the endpoints are nodes not in the SCC.  If so, that means that
	 * this edge connects to a part of the graph outside of this SCC and is
	 * thus a bridge edge.  That edge is then printed out. 
	 */
	private static void printBridges() {
		
		int counter = 0;
		
		while (!SCClist.isEmpty()) {
			
			LinkedList<Integer> temp = SCClist.remove(0);
			counter++;
			
			for (Integer i : temp) {
				
				Node n = graph.getNode(i);
				
				for (Integer e : n.edgeList) {
					if (!temp.contains(e)) {
						System.out.println("Bridge edge for SCC " + counter + ": " + i + "-" + e);
					}
				}
			}
		}
	}

	/**
	 * doTarjan() is the driver for strongConnect (the actual algorithm).
	 * It is called with one boolean argument that determines whether the
	 * algorithm should track singleton SCCs. 	
	 */
	private static void doTarjan(boolean trackSingletons) {
		
		index = 0;
		stack.clear();
		
		// Iterate through every node in the graph, calling strongConnect()
		// for that node if it has not yet been visited (index == -1).
		
		for (Integer i : graph.nodeList()) {
			
			Node n = graph.getNode(i);
			
			if (n.index == -1)
				strongConnect(n, trackSingletons);
		}
	}
	
	/**
	 * strongConnect() implements Tarjan's algorithm for tracking strongly-
	 * connected components.  It takes the node to start the DFS on and
	 * whether it should track singleton SCCs as arguments. 
	 */
	private static void strongConnect (Node n, boolean trackSingletons) {
		
		n.index = index;
		n.lowlink = index;
		index++;
		stack.push(n);		

		// Iterate through every node in the graph, calling strongConnect()
		// for that node if it has not yet been visited (index == -1).		
		
		for (int e : n.edgeList) {
			
			Node w = graph.getNode(e);
						
			// Step 1:
			//  Begin a DFS from the argument Node, recursively calling
			//  strongConnect() on nodes that have not yet been visited.
			
			if (w.index == -1) {				
				strongConnect(w, trackSingletons);
				n.lowlink = Math.min(n.lowlink, w.lowlink);
			}
			
			// Step 2:
			//  2) If the node has been visited, but is still in the stack,   
			//  update the lowlink value because this node is in the current
			//  SCC.
			
			else if (stack.contains(w)){
				n.lowlink = Math.min(n.lowlink, w.index);
			}
		}
		
		// Step 3:
		//  If lowlink == index, this is a root node of an SCC (e.g., the DFS
		//  returned to the starting node of a cycle).  Pop the nodes from the
		//  stack and print out this SCC.
		
		if (n.lowlink == n.index) {
			
			LinkedList<Integer> scc = new LinkedList<Integer>();
			Node w;
			
			do {
				w = stack.pop();				
				scc.add(w.name);
				
			} while (w.name != n.name);
			
			SCClist.add(scc);
			
			// If the SCC only contains one node, do not print it unless it
			// actually *is* a SCC (it has an edge to itself).  In this case,
			// the SCC is simply cleared.
			
			if (!trackSingletons && scc.size() == 1) {
				if (!graph.getNode(scc.get(0)).hasEdge(scc.get(0))) {
					scc.clear();
				}
			}
			
			// Print all SCCs >= size 1 or larger, unless singletons should
			// not be printed, in which case print SCCs >= size 2.
			
			if ((trackSingletons && scc.size() > 0) || (!trackSingletons && scc.size() > 1)) {
				
				System.out.println("Strongly connected component " + numSCCs + ": ");
				numSCCs++;
				
				for (Integer i : scc)
					System.out.print(i + " ");

				System.out.println();				
			}
		}
	}
	
	// Reset the index and lowlink values for all nodes in the graph between
	// doTarjan calls.
	
	public static void resetNodes () {
		
		for (Integer i : graph.nodeList()) {
			
			Node n = graph.getNode(i);
			n.index = -1;
			n.lowlink = -1;
		}
	}
	
	/**
	 * A simple helper class for a Cassandra database connection.
	 */
	static class DBHelper {
		
		List<KeySlice> results;
		TTransport transport;
		
		// Constructor.
		public DBHelper () throws Exception {

			// Set up connection.
			transport = new TFramedTransport(new TSocket("10.9.73.119", 9160));
			TProtocol protocol = new TBinaryProtocol(transport);
			Cassandra.Client client = new Cassandra.Client(protocol);
			transport.open();
			
			// Set the keyspace we are using.
			client.set_keyspace("CDM");

			// Set the parent of the column family
			ColumnParent parent = new ColumnParent();
			parent.column_family = COLUMN_FAMILY;

			// Set the consistency level
			ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;
		
			// Ask for 100 results from the specified column family (all results, in our case)
			SlicePredicate predicate = new SlicePredicate();
			predicate.setSlice_range(new SliceRange(ByteBuffer.wrap(new byte[0]), ByteBuffer.wrap(new byte[0]),
					false, 100));

			KeyRange keyRange = new KeyRange(100);
			keyRange.setStart_key(new byte[0]);
			keyRange.setEnd_key(new byte[0]);			
			
			// Perform the query
			results = client.get_range_slices(parent, predicate, keyRange, consistencyLevel);
		}
		
		// Gets all keys from the Edges column for the specified column family.
		public List<String> getEdges () throws Exception {
			
			List<String> returnList = new ArrayList<String>();
			
			// Add each query result into a list of Strings and return it.
			for (KeySlice ks : results) {
				returnList.add(new String (ks.getKey()));
			}
			
			return returnList;
		}
		
		// Closes the database connection.
		public void close () throws Exception {
			 
			// Clean up.
			transport.flush();
			transport.close();			
		}
	}
}