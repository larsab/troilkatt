package edu.princeton.function.troilkatt.fs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.log4j.Logger;

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
public class LogTableTarTest extends TestSuper {
	static final protected String tarName = "troilkatt-log-unitLogTable";
	protected static TroilkattNFS nfs;
	protected static Logger testLogger;
	protected LogTableTar logTable;
	static String rootLogDir;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {		
		testLogger = Logger.getLogger("test");
		rootLogDir = OsPath.join(outDir, "log/unitLogTable");
		OsPath.mkdir(rootLogDir);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		OsPath.deleteAll(rootLogDir);
	}

	@Before
	public void setUp() throws Exception {
		nfs = new TroilkattNFS();
		logTable = new LogTableTar("unitLogTable", nfs, OsPath.join(outDir, "log"), logDir, tmpDir);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testLogTable() throws PipelineException, IOException {
		assertEquals(nfs, logTable.tfs);
		assertNotNull(logTable.logger);
		assertEquals(rootLogDir, logTable.pipelineLogDir);
		assertEquals(logDir, logTable.myLogDir);
		assertEquals(tmpDir, logTable.tmpDir);
	}

	@Test
	public void testPutGetContainLogFiles() throws PipelineException, IOException, StageException, HbaseException {
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
				
		OsPath.delete(OsPath.join(logTable.pipelineLogDir, "004-unitTest/710.tar.gz"));
		
		// This can fail due to the above clearTable()
		assertEquals(4, logTable.putLogFiles("004-unitTest", 710, logFiles));
		assertTrue(OsPath.isfile(OsPath.join(logTable.pipelineLogDir, "004-unitTest/710.tar.bz2")));
		
		assertTrue(logTable.containsFile("004-unitTest", 710, "4.log"));
		assertFalse(logTable.containsFile("004-unitTest", 710, "6.log"));
		assertFalse(logTable.containsFile("004-unitTest", 711, "4.log"));
		
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
		assertNull(logFiles);
		
		// Invalid stage name
		logFiles = logTable.getLogFiles("004-unknown", 710, logDir);
		assertNull(logFiles);
		
		// Invalid timestamp
		logFiles = logTable.getLogFiles("004-unitTest", 717, logDir);
		assertNull(logFiles);
		
		// Invalid log table
		assertFalse(logTable.containsFile("444-unitTest", 710, "4.log"));
	}
	
	// Invalid input log file
	@Test
	public void testPutLogFiles2() throws PipelineException, IOException, StageException {
		ArrayList<String> logFiles = new ArrayList<String>();		
		logFiles.add("/invalid/filename");		
		String dstFile1 = OsPath.join(tmpDir, "1.log");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), dstFile1);
		logFiles.add(dstFile1);
		
		assertEquals(-1, logTable.putLogFiles("004-unitTest", 713, logFiles));
	}
	
	// Invalid output directory
	// Note. test assumes that testPutGetLogFiles is run before this test
	@Test(expected=StageException.class)
	public void testGetLogFiles2() throws PipelineException, IOException, StageException {		
		assertTrue(OsPath.isfile(OsPath.join(logTable.pipelineLogDir, "004-unitTest/710.tar.bz2")));
		assertEquals(0, logTable.getLogFiles("004-unknown", 710, "/invalud/dir"));
	}
}
