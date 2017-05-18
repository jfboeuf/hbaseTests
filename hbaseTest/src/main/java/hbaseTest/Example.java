package hbaseTest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;

public final class Example {

	private static final byte[] CF1 = Bytes.toBytes("CF1");
	private static final TableName TABLE_NAME = TableName.valueOf("MyTable");
	private static final byte[] COL1 = Bytes.toBytes("col1");

	public static void main(String... args) throws Throwable {
		Configuration config = HBaseConfiguration.create();

		// Add any necessary configuration files (hbase-site.xml, core-site.xml)
		config.addResource(new Path("/home/jef/hbase-1.2.5/conf/hbase-site.xml"));
		try (Connection connection = ConnectionFactory.createConnection(config); Admin admin = connection.getAdmin()) {
			if (!admin.tableExists(TABLE_NAME)) {

				HTableDescriptor table = new HTableDescriptor(TABLE_NAME);
				HColumnDescriptor newColumn = new HColumnDescriptor(CF1);
				// newColumn.setCompactionCompressionType(Algorithm.GZ);
				newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
				table.addFamily(new HColumnDescriptor(newColumn));
				admin.createTable(table);
				System.out.println("Table created");
			}
			long t0 = System.currentTimeMillis();
			HTable table = (HTable) connection.getTable(TABLE_NAME);
			// List<Put> puts = new ArrayList<>(100000);
			// for (int i = 0; i < 100000000; i++) {
			// Put put = new Put(Bytes.toBytes("key" + i % 10000000));
			// put.addColumn(CF1, COL1, Bytes.toBytes("value1-"
			// + i));
			// puts.add(put);
			// if (0 == i % 100000) {
			// System.err.println("100k");
			// table.put(puts);
			// puts.clear();
			// }
			// }
			// System.err.println("Inserted 100M records in " +
			// (System.currentTimeMillis()-t0)/1000 + "s.");
			Result row = table.get(new Get(Bytes.toBytes("key" + 123896)));
			Cell cell = row.getColumnCells(CF1, COL1).get(0);
			byte[] bvalue = CellUtil.cloneValue(cell);
			System.err.println(Bytes.toString(bvalue));
			System.err.println("gettime  " + (System.currentTimeMillis() - t0) + "ms.");
		}
	}
}