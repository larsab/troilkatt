package edu.princeton.function.troilkatt.fs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
import edu.princeton.function.troilkatt.hbase.HbaseException;
import edu.princeton.function.troilkatt.pipeline.StageException;

/**
 * 
 * NOTE! Many of the test fails when run "at full speed". This is due to clearTable() function used 
 * to clear a table. One soultion is to step through the tests in debug mode
 */
public class LogTableHbaseTest extends TestSuper {
	static final protected String tableName = "troilkatt-log-unitLogTable";
	static protected HBaseAdmin hbAdm;
	protected static Logger testLogger;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		Configuration hbConf = HBaseConfiguration.create();
		hbAdm = new HBaseAdmin(hbConf);
		testLogger = Logger.getLogger("test");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		//System.out.println("Break before each test (for debugging)");
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testLogTable() throws PipelineException, IOException {
		if (hbAdm.isTableAvailable(tableName)) {
			hbAdm.disableTable(tableName);
			hbAdm.deleteTable(tableName);
		}
		assertFalse(hbAdm.isTableAvailable(tableName));
		
		LogTableHbase logTable = new LogTableHbase("unitLogTable");
		assertEquals(tableName, logTable.tableName);
		assertNotNull(logTable.table);
		assertNotNull(logTable.hbConfig);
		assertNotNull(logTable.logger);
		
		assertTrue(hbAdm.isTableAvailable(tableName));
		assertTrue(HTable.isTableEnabled(tableName));
	}

	@Test
	public void testDeleteTable() throws PipelineException, IOException, InterruptedException, HbaseException {
		LogTableHbase logTable = new LogTableHbase("unitLogTable");
		assertTrue(hbAdm.isTableAvailable(tableName));
		
		logTable.schema.deleteTable();
		Thread.sleep(5000);
		// Note! Test may fail since table delete can return before the table is actually deleted
		assertFalse(hbAdm.isTableAvailable(tableName));
	}

	@Test
	public void testClearTable() throws PipelineException, IOException, HbaseException {
		LogTableHbase logTable = new LogTableHbase("unitLogTable");
		assertTrue(hbAdm.isTableAvailable(tableName));
		
		String content = "unitValue";
		Put put = new Put(Bytes.toBytes("unitRow"));
		put.add(Bytes.toBytes("log"), Bytes.toBytes("unitCol"), Bytes.toBytes(content));
		logTable.table.put(put);
		logTable.table.flushCommits();
		
		Get get = new Get(Bytes.toBytes("unitRow"));	
		get.addColumn(Bytes.toBytes("log"), Bytes.toBytes("unitCol"));
		assertTrue(logTable.table.exists(get));
		
		logTable.schema.clearTable();
		assertTrue(hbAdm.isTableAvailable(tableName));
		assertFalse(logTable.table.exists(get));
	}

	@Test
	public void testContainsFile() throws PipelineException, IOException, StageException, HbaseException {
		LogTableHbase logTable = new LogTableHbase("unitLogTable");
		// Note this line often causes the test to fail
		logTable.schema.clearTable();
		assertTrue(hbAdm.isTableAvailable(tableName));
		
		byte[] content = Bytes.toBytes("The content");
		Put put = new Put(Bytes.toBytes("001-unitTest.701"));
		put.add(Bytes.toBytes("log"), Bytes.toBytes("foo.log"), content);
		// If this statement fails the test, it is probably due to the above clearTable()
		logTable.table.put(put);
		put = new Put(Bytes.toBytes("002-unitTest2.701"));
		put.add(Bytes.toBytes("log"), Bytes.toBytes("foo.log"), content);
		logTable.table.put(put);
		put = new Put(Bytes.toBytes("001-unitTest.702"));
		put.add(Bytes.toBytes("log"), Bytes.toBytes("foo.log"), content);
		logTable.table.put(put);
		put = new Put(Bytes.toBytes("001-unitTest.701"));
		put.add(Bytes.toBytes("error"), Bytes.toBytes("foo.error"), content);
		put.add(Bytes.toBytes("out"), Bytes.toBytes("foo.out"), content);
		put.add(Bytes.toBytes("other"), Bytes.toBytes("foo"), content);
		logTable.table.put(put);		
		
		put = new Put(Bytes.toBytes("003-mapreduceUnitTest.701.task1"));
		put.add(Bytes.toBytes("log"), Bytes.toBytes("foo.log"), content);
		logTable.table.put(put);
		put = new Put(Bytes.toBytes("003-mapreduceUnitTest.701.task2"));
		put.add(Bytes.toBytes("log"), Bytes.toBytes("foo.log"), content);
		logTable.table.put(put);
		put = new Put(Bytes.toBytes("003-mapreduceUnitTest.701.task3"));
		put.add(Bytes.toBytes("log"), Bytes.toBytes("foo.log"), content);
		logTable.table.put(put);		
		
		assertTrue(logTable.containsFile("001-unitTest", 701, "foo.log"));
		assertTrue(logTable.containsFile("002-unitTest2", 701, "foo.log"));
		assertTrue(logTable.containsFile("001-unitTest", 702, "foo.log"));
		assertTrue(logTable.containsFile("001-unitTest", 701, "foo.error"));
		assertTrue(logTable.containsFile("001-unitTest", 701, "foo.out"));
		assertTrue(logTable.containsFile("001-unitTest", 701, "foo"));
		assertFalse(logTable.containsFile("003-unitTest", 701, "foo.log"));
		assertFalse(logTable.containsFile("001-unitTest2", 701, "foo.log"));
		assertFalse(logTable.containsFile("001-unitTest", 703, "foo.log"));
		assertFalse(logTable.containsFile("001-unitTest", 701, "bar.log"));
		assertFalse(logTable.containsFile("001-unitTest", 702, "foo"));
		assertTrue(logTable.containsFile("003-mapreduceUnitTest", 701, "task1", "foo.log"));
		assertTrue(logTable.containsFile("003-mapreduceUnitTest", 701, "task2", "foo.log"));
		assertTrue(logTable.containsFile("003-mapreduceUnitTest", 701, "task3", "foo.log"));
		assertFalse(logTable.containsFile("003- mapreduceUnitTest", 701, "task4", "foo.log"));
	}
	
	
	@Test
	public void testPutGetLogFiles() throws PipelineException, IOException, StageException, HbaseException {
		ArrayList<String> logFiles = new ArrayList<String>();
		String dstFile1 = OsPath.join(tmpDir, "1.log");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), dstFile1);
		logFiles.add(dstFile1);
		String dstFile2 = OsPath.join(tmpDir, "2.out");
		OsPath.copy(OsPath.join(dataDir, "files/file2"), dstFile2);
		logFiles.add(dstFile2);
		String dstFile3 = OsPath.join(tmpDir, "3.unknown");
		OsPath.copy(OsPath.join(dataDir, "files/file2"), dstFile3);
		logFiles.add(dstFile3);
		String dstFile4 = OsPath.join(tmpDir, "4.log");
		OsPath.copy(OsPath.join(dataDir, "files/file2"), dstFile4);
		logFiles.add(dstFile4);		
		
		LogTableHbase logTable = new LogTableHbase("unitLogTable");
		// Again a possible source of failure
		logTable.schema.clearTable();
		assertTrue(hbAdm.isTableAvailable(tableName));
		
		// This can fail due to the above clearTable()
		assertEquals(4, logTable.putLogFiles("004-unitTest", 710, logFiles));
		logTable.table.flushCommits();
		
		assertTrue(logTable.containsFile("004-unitTest", 710, "4.log"));
		assertFalse(logTable.containsFile("004-unitTest", 710, "6.log"));
		
		logFiles = logTable.getLogFiles("004-unitTest", 710, logDir);
		assertEquals(4, logFiles.size());
		Collections.sort(logFiles);
		assertTrue(logFiles.get(0).endsWith("1.log"));
		assertTrue(OsPath.isfile(OsPath.join(logDir, "1.log")));
		assertTrue(logFiles.get(1).endsWith("2.out"));
		assertTrue(logFiles.get(2).endsWith("3.unknown"));
		assertTrue(logFiles.get(3).endsWith("4.log"));
		
		// No logfiles to save
		assertEquals(0, logTable.putLogFiles("005-unitTest", 710, new ArrayList<String>()));
		
		// Invalid stage number
		logFiles = logTable.getLogFiles("005-unitTest", 710, logDir);
		assertEquals(0, logFiles.size());
		
		// Invalid stage name
		logFiles = logTable.getLogFiles("004-unknown", 710, logDir);
		assertEquals(0, logFiles.size());
		
		// Invalid timestamp
		logFiles = logTable.getLogFiles("004-unitTest", 717, logDir);
		assertEquals(0, logFiles.size());
	}
	
	// Invalid input log file
	@Test
	public void testPutLogFiles2() throws PipelineException, IOException, StageException {
		ArrayList<String> logFiles = new ArrayList<String>();		
		logFiles.add("/invalid/filename");		
		String dstFile1 = OsPath.join(tmpDir, "1.log");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), dstFile1);
		logFiles.add(dstFile1);
		
		LogTableHbase logTable = new LogTableHbase("unitLogTable");		
		assertTrue(hbAdm.isTableAvailable(tableName));
		
		assertEquals(1, logTable.putLogFiles("004-unitTest", 713, logFiles));
	}
	
	// Invalid output directory
	@Test(expected=StageException.class)
	public void testGetLogFiles2() throws PipelineException, IOException, StageException {		
		LogTableHbase logTable = new LogTableHbase("unitLogTable");		
		assertTrue(hbAdm.isTableAvailable(tableName));		
		assertEquals(0, logTable.getLogFiles("004-unknown", 710, "/invalud/dir"));
	}

}
