package edu.princeton.function.troilkatt.fs;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class HbaseSetupTest {

	@Test
	public void test() throws IOException {
		theTest();
	}
	
	public static void theTest() throws IOException {
		Configuration hbConf = HBaseConfiguration.create();
		String tableName = "troilkatt-log-configTest";
		HTable table;
		String[] colFams = {"fam1", "fam2", "fam3"};

		/*
		 * Open an possibly create table
		 */
		try {
			table = new HTable(hbConf, tableName);
		} catch (TableNotFoundException e1) {				
			System.err.println("LogTable not found for pipeline: " + tableName);

			HBaseAdmin hbAdm = new HBaseAdmin(hbConf);
			HTableDescriptor htd = new HTableDescriptor(Bytes.toBytes(tableName));		
			for (String f: colFams) {
				HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toBytes(f));
				// Cache MapFile blocks in memory
				hcd.setBlockCacheEnabled(true);
				// But do not attempt to load all blocks in a family to memory (default) 			
				hcd.setInMemory(false);
				// Just use default block size (64KB)
				hcd.setBlocksize(65336); // 64KB				
				// Use a Bloom filter for row keys
				hcd.setBloomFilterType(StoreFile.BloomType.ROW);		
				// Use gzip for best compression ratio (but slower read/write) since data is mostly
				// write only
				hcd.setCompressionType(Compression.Algorithm.GZ );
				hcd.setCompactionCompressionType(Compression.Algorithm.GZ);			
				// Keep a couple of version for testing and debugging purposes
				hcd.setMaxVersions(1);							
				// Values should be kept forever 
				hcd.setTimeToLive(HConstants.FOREVER);		
				hcd.setScope(0);

				htd.addFamily(hcd);
			}

			hbAdm.createTable(htd);
			hbAdm.close();

			// attempt to re-open table
			table = new HTable(hbConf, tableName);		
		}

		// Do not enable client side write buffer (default): if used rows must be explicitly flushed after a put
		table.setAutoFlush(true);

		/* Enable client side scanner caching: multiple rows are retrieved at a time for
		 * a scanner and cached in the client memory. This will also increase the client
		 * memory footprint */
		// Should be done for each scanner
		//table.setScannerCaching(10);


		/*
		 * Write some data
		 */
		byte[] content = Bytes.toBytes("The content");
		Put put = new Put(Bytes.toBytes("row1"));
		put.add(Bytes.toBytes("fam1"), Bytes.toBytes("col1"), content);
		table.put(put);

		/*
		 * Read data
		 */
		Get get = new Get(Bytes.toBytes("row1"));
		get.addColumn(Bytes.toBytes("fam1"), Bytes.toBytes("col1"));
		Result result = table.get(get);
		assertNotNull(result);
		byte[] valBytes = result.getValue(Bytes.toBytes("fam1"), Bytes.toBytes("col1"));
		assertNotNull(valBytes);
		String val = Bytes.toString(valBytes);
		assertEquals("The content", val);
		
		table.close();
	}
	
	public static void main(String[] args) throws IOException {
		HbaseSetupTest.theTest();
	}
}
