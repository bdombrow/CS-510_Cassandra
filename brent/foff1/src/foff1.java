import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;
import java.util.TreeSet;

import org.apache.cassandra.thrift.*;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;

public class foff1 {
	public static void main(String[] args) throws Exception {
		
		String startNode = "1";
	
		// Set up decoder.
		Charset charset = Charset.forName("UTF-8");
		CharsetDecoder decoder = charset.newDecoder();
	
		// Set up connection.
		TTransport transport = new TFramedTransport(new TSocket("10.9.73.119", 9160));
		TProtocol protocol = new TBinaryProtocol(transport);
		Cassandra.Client client = new Cassandra.Client(protocol);
		transport.open();
		
		// Set the keyspace we are using.
		client.set_keyspace("CDM");

		// Set the parent of the column family
		ColumnParent parent = new ColumnParent("Edges");

		// Set the consistency level
		ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;	
	
		// Set up the predicate for what we want out of the query
		SlicePredicate predicate = new SlicePredicate();
		predicate.addToColumn_names(ByteBuffer.wrap("start".getBytes()));
		predicate.addToColumn_names(ByteBuffer.wrap("end".getBytes()));
		
		// Set the expression for the index query
		IndexExpression indexExpression = new IndexExpression();
		indexExpression.column_name = ByteBuffer.wrap("start".getBytes("UTF-8"));
		indexExpression.setOp(IndexOperator.EQ);
		indexExpression.value = ByteBuffer.wrap(startNode.getBytes());
		
		// Create an IndexClause and add the index query to it.
		IndexClause indexClause = new IndexClause();
		indexClause.addToExpressions(indexExpression);
		indexClause.start_key = ByteBuffer.allocate(0);

		// Get the results.
		List<KeySlice> rangeResults = client.get_indexed_slices(parent, indexClause, predicate, consistencyLevel);

		// The friends set.
		TreeSet<String> friends = new TreeSet<String>();
		
		// Print them out.
		//System.out.println("Friends\n");
		for (KeySlice key : rangeResults) {
			//System.out.println(decoder.decode(key.bufferForKey()));
			for (ColumnOrSuperColumn col : key.columns) {
				String colName = decoder.decode(col.column.name).toString();
				String colValue = decoder.decode(col.column.value).toString();
				//System.out.println("\t" + colName + " -> " + colValue);
				if (colName.contentEquals("end")) {
					friends.add(colValue);
				}
			}
		}
		
		// The friends of friends set.
		TreeSet<String> foff = new TreeSet<String>();
		
		// Get the friends of the friends.
		//System.out.println("\nFriends of Friends\n");
		for (String friend : friends) {
			indexExpression.value = ByteBuffer.wrap(friend.getBytes());
			
			rangeResults = client.get_indexed_slices(parent, indexClause, predicate, consistencyLevel);
			
			// Print them out.
			for (KeySlice key : rangeResults) {
				//System.out.println(decoder.decode(key.bufferForKey()));
				for (ColumnOrSuperColumn col : key.columns) {
					String colName = decoder.decode(col.column.name).toString();
					String colValue = decoder.decode(col.column.value).toString();
					//System.out.println("\t" + colName + " -> " + colValue);
					if (colName.contentEquals("end")) {
						foff.add(colValue);
					}
				}
			}
		}
		
		// Get the results.
		rangeResults = client.get_indexed_slices(parent, indexClause, predicate, consistencyLevel);
		
		// Print them out.
		for (KeySlice key : rangeResults) {
			//System.out.println(decoder.decode(key.bufferForKey()));
			for (ColumnOrSuperColumn col : key.columns) {
				String colName = decoder.decode(col.column.name).toString();
				String colValue = decoder.decode(col.column.value).toString();
				//System.out.println("\t" + colName + " -> " + colValue);
				if (colName.contentEquals("end")) {
					friends.add(colValue);
				}
			}
		}
		
		System.out.println("Friends of friends for " + startNode + " are the following:");
		for (String f : foff) {
			System.out.println("\t" + f);
		}
		
		// Clean up.
		transport.flush();
		transport.close();
	}
}