import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;

import org.apache.cassandra.thrift.*;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;

public class foff1 {
	public static void main(String[] args) throws Exception {
	
		Charset charset = Charset.forName("UTF-8");
		CharsetDecoder decoder = charset.newDecoder();
	
		TTransport transport = new TFramedTransport(new TSocket("10.9.73.119", 9160));
		TProtocol protocol = new TBinaryProtocol(transport);
		Cassandra.Client client = new Cassandra.Client(protocol);
		transport.open();
		
		client.set_keyspace("tutorials");

		// Define a column parent
		ColumnParent parent = new ColumnParent("User");
		
		// Define a row id
		ByteBuffer rowid = ByteBuffer.wrap("100".getBytes());

		// Define a column to add
		Column tempColumn = new Column();
		tempColumn.setName("description".getBytes("UTF-8"));
		tempColumn.setValue("I'm a nice guy".getBytes("UTF-8"));
		tempColumn.setTimestamp(System.currentTimeMillis());

		// Set the consistency level
		ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;

		// Insert the value
		client.insert(rowid, parent, tempColumn, consistencyLevel);
		
		tempColumn.setName("username".getBytes("UTF-8"));
		tempColumn.setValue("joe".getBytes("UTF-8"));
		
		client.insert(rowid, parent, tempColumn, consistencyLevel);
		
		// Read a column value
		// The output is messed up. Need to figure out how to convert to string properly.
//		ColumnPath path = new ColumnPath();
//		path.column_family = "User";
//		path.column = ByteBuffer.wrap("description".getBytes());
//		ColumnOrSuperColumn answer = client.get(rowid, path, consistencyLevel);
//		Column column = answer.column;
//		System.out.println(decoder.decode(column.name) + ":" + decoder.decode(column.value));
		
		// Read Entire row
		SlicePredicate predicate = new SlicePredicate();
		SliceRange range = new SliceRange();
		ByteBuffer start = ByteBuffer.allocate(0);
		ByteBuffer end = ByteBuffer.allocate(0);
		range.start = start;
		range.finish = end;
		predicate.slice_range = range;
		
//		List<ColumnOrSuperColumn> results = client.get_slice(rowid, parent, predicate, consistencyLevel);
//		
//		for (ColumnOrSuperColumn result : results) {
//			Column columna = result.column;
//			System.out.println(decoder.decode(columna.name) + " -> " + decoder.decode(columna.value));
//		}
//		
		// Range query
		KeyRange keyrange = new KeyRange();
		keyrange.start_token = "0";
		keyrange.end_token = "100";
		
		List<KeySlice> rangeResults = client.get_range_slices(parent, predicate, keyrange, consistencyLevel);
		
		for (KeySlice key : rangeResults) {
			System.out.println(decoder.decode(key.bufferForKey()));
			for (ColumnOrSuperColumn col : key.columns) {
				System.out.println(decoder.decode(col.column.name) + " -> " + decoder.decode(col.column.value));
			}
		}
		
		
		transport.flush();
		transport.close();
	}
}