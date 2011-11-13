import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import org.apache.cassandra.thrift.*;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;


public class tc1 {
	public static void main(String[] args) throws Exception {
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

		ByteBuffer query = ByteBuffer.wrap("SELECT start, end FROM Edges;".getBytes());

		CqlResult results = client.execute_cql_query(query, Compression.NONE);
		
		for (CqlRow row : results.getRows()) {
			System.out.println("");
			for (Column col : row.getColumns()) {
				System.out.println(decoder.decode(col.name) + ":" + decoder.decode(col.value));		
			}
			
		}
		
		transport.flush();
		transport.close();
	}
}
