package edu.princeton.function.troilkatt.hbase;

import static org.junit.Assert.*;

import java.io.IOException;
import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.princeton.function.troilkatt.PipelineException;
import edu.princeton.function.troilkatt.TestSuper;

public class TableTest extends TestSuper {
	static final protected String tableName = "unitTable";
	static protected Configuration hbConf;
	static protected HBaseAdmin hbAdm;
	protected static Logger testLogger;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		hbConf = HBaseConfiguration.create();
		hbAdm = new HBaseAdmin(hbConf);
		testLogger = Logger.getLogger("test");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGetConfiguration() {
		assertNotNull(TroilkattTable.getConfiguration());
	}

	@Test
	public void testCreateTable() throws IOException, HbaseException {
		createTable();
	}

	@Test
	public void testOpenTable() throws IOException, HbaseException {
		if (hbAdm.isTableAvailable(tableName)) {
			hbAdm.disableTable(tableName);
			hbAdm.deleteTable(tableName);
		}
		assertFalse(hbAdm.isTableAvailable(tableName));
		
		TroilkattTable table = new TroilkattTable();
		assertNull(table.tableName);
		assertNull(table.table);
		
		table.tableName = tableName;
		table.colFams = new String[1];
		table.colFams[0] = "fam";
		
		Configuration hbConf = TroilkattTable.getConfiguration();
		table.openTable(hbConf, true);
		
		assertNotNull(table.table);
		
		assertTrue(hbAdm.isTableAvailable(tableName));
		assertTrue(HTable.isTableEnabled(tableName));
		
		hbAdm.disableTable(tableName);
		hbAdm.deleteTable(tableName);
		assertFalse(hbAdm.isTableAvailable(tableName));
	}
	
	@Test(expected=HbaseException.class)
	public void testOpenTable2() throws IOException, HbaseException { 
		if (hbAdm.isTableAvailable(tableName)) {
			hbAdm.disableTable(tableName);
			hbAdm.deleteTable(tableName);
		}
		assertFalse(hbAdm.isTableAvailable(tableName));
		
		TroilkattTable table = new TroilkattTable();
		assertNull(table.tableName);
		assertNull(table.table);
		
		table.tableName = tableName;
		table.colFams = new String[1];
		table.colFams[0] = "fam";
		
		Configuration hbConf = TroilkattTable.getConfiguration();
		table.openTable(hbConf, false);
	}

	@Test
	public void testDeleteTable() throws PipelineException, IOException, InterruptedException, HbaseException {
		TroilkattTable table = createTable();
		table.openTable(hbConf, false);
		
		table.deleteTable();
		Thread.sleep(5000);
		// Note! Test may fail since table delete can return before the table is actually deleted
		assertFalse(hbAdm.isTableAvailable(tableName));
	}

	@Test
	public void testClearTable() throws PipelineException, IOException, HbaseException {
		TroilkattTable table = createTable();
		table.openTable(hbConf, false);
		
		String content = "unitValue";
		Put put = new Put(Bytes.toBytes("unitRow"));
		put.add(Bytes.toBytes("fam"), Bytes.toBytes("unitCol"), Bytes.toBytes(content));
		table.table.put(put);
		
		Get get = new Get(Bytes.toBytes("unitRow"));	
		get.addColumn(Bytes.toBytes("fam"), Bytes.toBytes("unitCol"));
		assertTrue(table.table.exists(get));
		
		table.clearTable();
		assertTrue(hbAdm.isTableAvailable(tableName));
		assertFalse(table.table.exists(get));
	}

	private TroilkattTable createTable() throws IOException, HbaseException {
		if (hbAdm.isTableAvailable(tableName)) {
			hbAdm.disableTable(tableName);
			hbAdm.deleteTable(tableName);
		}
		assertFalse(hbAdm.isTableAvailable(tableName));
		
		TroilkattTable table = new TroilkattTable();
		assertNull(table.tableName);
		assertNull(table.table);
		
		table.tableName = tableName;
		table.colFams = new String[1];
		table.colFams[0] = "fam";
		
		table.createTable(hbConf);
		
		assertTrue(hbAdm.isTableAvailable(tableName));
		assertTrue(HTable.isTableEnabled(tableName));
		
		return table;
	}
}
