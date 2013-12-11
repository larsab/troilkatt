package edu.princeton.function.troilkatt.hbase;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Superclass for HTable schemas. Each Troilkatt table schema is specified in a
 * subclass.
 */
public class TroilkattTable {
	/* 
	 * These fields should be set in the constructor of the subclass.
	 */
	// Hbase table name
	public String tableName;
	// Hbase column family names
	public String[] colFams;
	
	/* 
	 * Default values: subclass can overwrite if needed
	 */
	// Cache MapFile blocks in memory
	protected boolean blockCacheEnabled = true;
	// Do not attempt to load all blocks in a family to memory (false is Hbase default)
	protected boolean inMemory = false;
	// Default block size (64KB is Hbase default)
	protected int blockSize = 65336; // 64KB
	// Use a Bloom filter on row keys
	protected BloomType bloomFilterType = StoreFile.BloomType.ROW;
	// Us gzip for high compression ratio 
	protected Algorithm compressionType = Compression.Algorithm.GZ;
	protected Algorithm compactionCompressionType = Compression.Algorithm.GZ;
	// Number of cell versions to keep is by default one		
	protected int maxVersions = 1;
	// Time to keep values (Hbase default is forever)
	protected int timeToLive = HConstants.FOREVER;
	// We do not have multiple clusters so the replication scope is zero (Hbase default)
	protected int scope = 0;
	
	/*
	 * Table instance that is set in openTable
	 */
	public HTable table;

	/**
	 * Open a table. If the table does not exist it is created.
	 * 
	 * @param hbc Hbase configuration object
	 * @param createIfNeeded true if the table should be created if it does
	 * not exists.
	 * @return table handler
	 * @throws HbaseException if an Hbase-related exception occurs 
	 */
	public HTable openTable(Configuration hbConfig, boolean createIfNeeded) throws HbaseException {			
		try {
			try {
				table = new HTable(hbConfig, tableName);
			} catch (TableNotFoundException e1) {				
				System.err.println("LogTable not found for pipeline: " + tableName);
				if (createIfNeeded) {
					createTable(hbConfig);
					// attempt to re-open table
					table = new HTable(hbConfig, tableName);
				}
				else {
					throw new HbaseException("Could not open table (may not exist): " + tableName);
				}
			}
		} catch (MasterNotRunningException e) {			
			throw new HbaseException("HBaseMaster is not running: " + e.toString());
		} catch (ZooKeeperConnectionException e) {			
			throw new HbaseException("Could not connect to ZooKeeper: " + e.toString());
		} catch (IOException e) {			
			throw new HbaseException("Could not open or create table" + tableName + " IOException:" + e.getMessage());
		}
		
		// Do not enable client side write buffer (default): if used rows must be explicitly flushed after a put
		table.setAutoFlush(true);
		
		/* Enable client side scanner caching: multiple rows are retrieved at a time for
		 * a scanner and cached in the client memory. This will also increase the client
		 * memory footprint */
		// Should be done for each scanner
		// table.setScannerCaching(10);
		
		return table;
	}
	
	/**
	 * Create a Hbase table.
	 *
	 * @param hbc Hbase configuation object
	 * @throws HbaseException 
	 */
	public void createTable(Configuration hbConfig) throws HbaseException {
		try {
			HBaseAdmin hbAdm = new HBaseAdmin(hbConfig);
			HTableDescriptor htd = new HTableDescriptor(Bytes.toBytes(tableName));		
			for (String f: colFams) {
				HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toBytes(f));
				// Cache MapFile blocks in memory
				hcd.setBlockCacheEnabled(blockCacheEnabled);
				// But do not attempt to load all blocks in a family to memory (default) 			
				hcd.setInMemory(inMemory);
				// Just use default block size (64KB)
				hcd.setBlocksize(blockSize); // 64KB				
				// Use a Bloom filter for row keys
				hcd.setBloomFilterType(bloomFilterType );		
				// Use gzip for best compression ratio (but slower read/write) since data is mostly
				// write only
				hcd.setCompressionType(compressionType );
				hcd.setCompactionCompressionType(compactionCompressionType);			
				// Keep a couple of version for testing and debugging purposes
				hcd.setMaxVersions(maxVersions);							
				// Values should be kept forever 
				hcd.setTimeToLive(timeToLive);		
				hcd.setScope(scope);
				
				htd.addFamily(hcd);
			}
			
			hbAdm.createTable(htd);
			hbAdm.close();
		} catch (MasterNotRunningException e) {
			throw new HbaseException("HBaseMaster is not running: " + e.toString());
		} catch (ZooKeeperConnectionException e) {
			throw new HbaseException("Could not connect to ZooKeeper: " + e.toString());
		} catch (IOException e) {
			throw new HbaseException("Could not open or create table" + tableName + " IOException:" + e.getMessage());
		}
	}
	
	/**
	 * Delete a table. Mostly used for testing.
	 * 
	 * @param hbConfig Initialized HBase configuration object 
	 * 
	 * @throws HbaseException if the table could not be deleted
	 */
	public void deleteTable(Configuration hbConfig) throws HbaseException {
		try {
			table.close();
			HBaseAdmin hbAdm = new HBaseAdmin(hbConfig);
			hbAdm.disableTable(tableName);
			hbAdm.deleteTable(tableName);		
			hbAdm.close();
		} catch (MasterNotRunningException e) {
			throw new HbaseException("HBaseMaster is not running: " + e.toString());
		} catch (ZooKeeperConnectionException e) {
			throw new HbaseException("Could not connect to ZooKeeper: " + e.toString());
		} catch (IOException e) {
			throw new HbaseException("Could not delete table " + tableName + ": IOEXception: " + e.getMessage());
		}
	}

	/**
	 * Delete and recreate table. Intended for testing.
	 * 
	 * @param hbConfig Initialized HBase configuration object
	 * @throws HbaseException 
	 */
	public void clearTable(Configuration hbConfig) throws HbaseException {
		try {
			HBaseAdmin hbAdm = new HBaseAdmin(hbConfig);
			table.close();
			hbAdm.disableTable(tableName);
			hbAdm.deleteTable(tableName);
			createTable(hbConfig);			
			hbAdm.close();
		} catch (MasterNotRunningException e) {
			throw new HbaseException("HBaseMaster is not running: " + e.toString());
		} catch (ZooKeeperConnectionException e) {
			throw new HbaseException("Could not connect to ZooKeeper: " + e.toString());
		} catch (IOException e) {
			throw new HbaseException("Could not clear table " + tableName + ": IOEXception: " + e.getMessage());
		}
		
		try {
			table = new HTable(hbConfig, tableName);
		} catch (IOException e) {
			throw new HbaseException("Could not create table: " + e.getMessage());
		}
	}
	
	/**
	 * Convert an ArrayList<String> to a single String where the array elements are seperated with newlines.
	 * This function will also convert any newlines in the array elements to <NEWLINE>. The last element does
	 * not have a newline
	 * 
	 * @param array to convert to string
	 * @return String as described above. If the array is empty an empty string ("") is returned. If
	 * array is null then null is returned 
	 */
	public static String array2string(ArrayList<String> array) {
		if (array == null) {
			return null;
		}
		if (array.size() == 0) {
			return "";
		}
		
		String s = array.get(0).replace("\n", "<NEWLINE>"); // newline is used as seperator
		for (int i = 1; i < array.size(); i++) {
			s = s + "\n" + array.get(i).replace("\n", "<NEWLINE>");
		}
		return s;
	}
	
	/**
	 * Convert String to an ArrayList<String>. The String is assumed to have been created using the
	 * array2string method. The String contains array elements seperated with newlines.
	 * This function will also convert any <NEWLINE> in the array elements to newline (\n). 
	 * 
	 * @param string to convert to an array
	 * @return ArrayList as described above, or null if the String is null
	 */
	public static ArrayList<String> string2array(String string) {
		if (string == null) {
			return null;
		}
		
		ArrayList<String> array = new ArrayList<String>();
		if (string.isEmpty()) {
			return array;
		}
		
		String parts[] = string.split("\n");
		for (String p: parts) {
			array.add(p.replace("<NEWLINE>", "\n"));
		}

		return array;
	}
}
