/**
 * 
 */
package edu.princeton.function.troilkatt;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import org.apache.log4j.Logger;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFS;
import edu.princeton.function.troilkatt.utils.Utils;

public class TroilkattTest extends TestSuper {
	// Set in setUp
	protected String logDir = null;
	
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		initTestDir();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		logDir = OsPath.join(tmpDir, "logs");
		if (OsPath.isdir(logDir)) {
			OsPath.rmdir(logDir);
		}
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link edu.princeton.function.troilkatt.Troilkatt#Troilkatt()}.
	 */
	@Test
	public void testTroilkatt() {
		Troilkatt t = new Troilkatt();
		assertNotNull(t.DEFAULT_ARGS.get("configFile"));
		assertNotNull(t.DEFAULT_ARGS.get("datasetFile"));
		assertNotNull(t.DEFAULT_ARGS.get("logProperties"));
		assertNotNull(t.tfs);
		// Set in run()
		assertNull(t.status);
	}

	/**
	 * Test method for {@link edu.princeton.function.troilkatt.Troilkatt#getYesOrNo(java.lang.String, boolean)}.
	 */
	//@Test
	public void testGetYesOrNo() {
		// Yes'
		assertTrue(Troilkatt.getYesOrNo("Enter 'y'", true));
		assertFalse(Troilkatt.getYesOrNo("Enter 'n'", true));
		assertTrue(Troilkatt.getYesOrNo("Enter 'Y'", true));		
		assertTrue(Troilkatt.getYesOrNo("Enter 'yes'", true));
		assertTrue(Troilkatt.getYesOrNo("Enter 'Yes'", true));
		assertTrue(Troilkatt.getYesOrNo("Press Enter", true));
		
		// No's
		assertFalse(Troilkatt.getYesOrNo("Enter 'n'", true));
		assertTrue(Troilkatt.getYesOrNo("Enter 'Y'", true));
		assertFalse(Troilkatt.getYesOrNo("Enter 'N'", true));		
		assertFalse(Troilkatt.getYesOrNo("Enter 'no'", true));
		assertFalse(Troilkatt.getYesOrNo("Enter 'No'", true));
		assertFalse(Troilkatt.getYesOrNo("Press Enter", false));
	
		// Invalid input
		assertTrue(Troilkatt.getYesOrNo("Enter 'foo' then 'y'", true));
		assertFalse(Troilkatt.getYesOrNo("Enter 'foo', then 'bar', and finally press Enter", false));
	}

	/**
	 * Test method for {@link edu.princeton.function.troilkatt.Troilkatt#usage(java.lang.String)}.
	 * Nothing to test
	 */
	//@Test
	//public void testUsage() {
	//}

	/**
	 * Test method for {@link edu.princeton.function.troilkatt.Troilkatt#parseArgs(java.lang.String[])}.
	 */
	@Test
	public void testParseArgs() {
		Troilkatt t = new Troilkatt();
		
		String[] args1 = {"-c", "configArg", "-d", "datasetArg", "-l", "loggingArg", "-s", "recovery", "-o", "cleanup"};
		HashMap<String, String> argDict = t.parseArgs(args1);
		assertEquals(5, argDict.size());
		assertEquals("configArg", argDict.get("configFile"));
		assertEquals("datasetArg", argDict.get("datasetFile"));
		assertEquals("loggingArg", argDict.get("logProperties"));
		assertEquals("recovery", argDict.get("skip"));
		assertEquals("cleanup", argDict.get("only"));
		
		String[] args2 = {"-d", "datasetArg", "-c", "configArg", "-o", "recovery"};
		argDict = t.parseArgs(args2);
		assertEquals(5, argDict.size());
		assertEquals("configArg", argDict.get("configFile"));
		assertEquals("datasetArg", argDict.get("datasetFile"));
		assertEquals(t.DEFAULT_ARGS.get("logProperties"), argDict.get("logProperties"));
		assertEquals(t.DEFAULT_ARGS.get("skip"), argDict.get("skip"));
		assertEquals("recovery", argDict.get("only"));
		
		String[] args3 = {"-c", "configArg", "-s", "cleanup"};
		argDict = t.parseArgs(args3);
		assertEquals(5, argDict.size());
		assertEquals("configArg", argDict.get("configFile"));
		assertEquals(t.DEFAULT_ARGS.get("datasetFile"), argDict.get("datasetFile"));
		assertEquals(t.DEFAULT_ARGS.get("logProperties"), argDict.get("logProperties"));
		assertEquals("cleanup", argDict.get("skip"));
		assertEquals(t.DEFAULT_ARGS.get("only"), argDict.get("only"));
		
		String[] args4 = {};
		argDict = t.parseArgs(args4);
		assertEquals(5, argDict.size());
		assertEquals(t.DEFAULT_ARGS.get("configFile"), argDict.get("configFile"));
		assertEquals(t.DEFAULT_ARGS.get("datasetFile"), argDict.get("datasetFile"));
		assertEquals(t.DEFAULT_ARGS.get("logProperties"), argDict.get("logProperties"));
		assertEquals(t.DEFAULT_ARGS.get("skip"), argDict.get("skip"));
		assertEquals(t.DEFAULT_ARGS.get("only"), argDict.get("only"));
		
		String[] args5 = {"-c", "configArg", "-f", "foo", "-d", "datasetArg", "-b", "bar"};
		argDict = t.parseArgs(args5);
		assertEquals(5, argDict.size());
		assertEquals("configArg", argDict.get("configFile"));
		assertEquals("datasetArg", argDict.get("datasetFile"));
		assertEquals(t.DEFAULT_ARGS.get("logProperties"), argDict.get("logProperties"));
		assertEquals(t.DEFAULT_ARGS.get("skip"), argDict.get("skip"));
		assertEquals(t.DEFAULT_ARGS.get("only"), argDict.get("only"));
	
		// Will exit
		//System.err.println("This should call exit");
		//String[] args6 = {"-h"};
		//argDict = t.parseArgs(args6);
		
		// Will exit
		//System.err.println("This should call exit");
		//String[] args7 = {"-s", "foo"};
		//argDict = t.parseArgs(args7);
		
		// Will exit
		//System.err.println("This should call exit");
		//String[] args8 = {"-o", "bar"};
		//argDict = t.parseArgs(args8);
		
		// Will exit
		//System.err.println("This should call exit");
		//String[] args9 = {"-s", "recovery", "-o", "recovery"};
		//argDict = t.parseArgs(args9);
	}

	/**
	 * Test method for {@link edu.princeton.function.troilkatt.Troilkatt#getProperties(java.lang.String)}.
	 * 
	 * Note: parser test is done in TroilkattPropertiesTest
	 */
	@Test
	public void testGetProperties() {			
		assertNotNull(Troilkatt.getProperties(OsPath.join(dataDir, configurationFile)));		
	}
	
	@Test(expected=RuntimeException.class)
	public void testGetProperties2() {
		Troilkatt.getProperties(OsPath.join(dataDir, "invalidConfig.xml"));
	}
	
	@Test(expected=RuntimeException.class)
	public void testGetProperties3() {
		Troilkatt.getProperties(OsPath.join(dataDir, "nonExistingConfig.xml"));
	}

	/**
	 * Test method for {@link edu.princeton.function.troilkatt.Troilkatt#setupLogging(java.lang.String, java.lang.String, java.lang.String)}.
	 * 
	 * Note! This test requires manually checking the resulting logfiles
	 */
	@Test
	public void testSetupLogging() throws IOException {		
		Troilkatt.setupLogging(OsPath.join(dataDir, logProperties));
		Logger logger = Logger.getLogger(TroilkattTest.class);
		logMsgs(logger);
	}
	
	// In case of an invalid log properties file only an error message is printed 
	@Test
	public void testSetupLogging2() throws IOException {
		Troilkatt.setupLogging("finest");
	}
	
	private void logMsgs(Logger logger) {
		logger.trace("Trace message");
		logger.debug("Debug message");
		logger.info("Info message");		
		logger.warn("Warning message");
		logger.error("Error message");
		logger.fatal("Fatal message");
	}

	/**
	 * Test method for {@link edu.princeton.function.troilkatt.Troilkatt#getLastTroilkattStatus()}.
	 * @throws IOException 
	 * @throws TroilkattPropertiesException 
	 */
	@Test
	public void testGetLastTroilkattStatus() throws IOException, TroilkattPropertiesException {
		Troilkatt t = new Troilkatt();
		TroilkattProperties p = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));				
		Path hdfsStatusPath = new Path(p.get("troilkatt.hdfs.status.file"));
		String statusFilename = OsPath.join(p.get("troilkatt.localfs.dir"), hdfsStatusPath.getName());
		OsPath.delete(statusFilename);
		
		t.status = new TroilkattStatus(t.tfs, p);
		t.status.statusFilename = statusFilename;
		t.status.statusPath = new Path(statusFilename);
		assertEquals(4, t.getLastTroilkattStatus());
	}
	
	@Test(expected=IOException.class)
	public void testGetLastTroilkattStatus2() throws IOException, TroilkattPropertiesException {
		Troilkatt t = new Troilkatt();
		TroilkattProperties p = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));						
		String statusFilename = "invalid-filename";		
		t.status = new TroilkattStatus(t.tfs, p);
		t.status.statusFilename = statusFilename;
		t.status.statusPath = new Path(statusFilename);
		assertEquals(4, t.getLastTroilkattStatus());
	}
	
	@Test
	public void testVerifyCreateTroilkattDirs() throws TroilkattPropertiesException, IOException {
		Troilkatt t = new Troilkatt();
		TroilkattProperties p = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));
		
		String ctdt = OsPath.join(tmpDir, "create-troilkatt-dirs-test");
		p.set("troilkatt.localfs.dir", OsPath.join(ctdt, "root"));
		p.set("troilkatt.localfs.log.dir",  OsPath.join(ctdt, "log"));			
		p.set("troilkatt.globalfs.global-meta.dir",  OsPath.join(ctdt, "global"));
		p.set("troilkatt.localfs.mapreduce.dir", OsPath.join(ctdt, "mapred"));
		p.set("troilkatt.localfs.binary.dir", OsPath.join(ctdt, "bin"));
		p.set("troilkatt.localfs.utils.dir", OsPath.join(ctdt, "utils"));
		p.set("troilkatt.localfs.scripts.dir", OsPath.join(ctdt, "scripts"));
		p.set("troilkatt.hdfs.root.dir", OsPath.join(hdfsRoot, "create-troilkatt-dirs-test"));
				
		t.tfs.deleteDir(p.get("troilkatt.hdfs.root.dir"));		
		OsPath.deleteAll(ctdt);
		assertFalse(t.verifyTroilkattDirs(p));
				
		t.createTroilkattDirs(p);
		// Not created by above function
		OsPath.mkdir(p.get("troilkatt.localfs.binary.dir"));
		OsPath.mkdir(p.get("troilkatt.localfs.utils.dir"));
		OsPath.mkdir(p.get("troilkatt.localfs.scripts.dir"));
		assertTrue(t.verifyTroilkattDirs(p));
		
		t.tfs.deleteDir(p.get("troilkatt.hdfs.root.dir"));
		assertFalse(t.verifyTroilkattDirs(p));
		
		t.createTroilkattDirs(p);
		assertTrue(t.verifyTroilkattDirs(p));
		
		OsPath.deleteAll(ctdt);
		assertFalse(t.verifyTroilkattDirs(p));
		
		t.tfs.deleteDir(p.get("troilkatt.hdfs.root.dir"));
	}

	@Test
	public void testDownloadGlobalMeta() throws IOException {
		Troilkatt troilkatt = new Troilkatt();
		troilkatt.logger = Logger.getLogger("troilkatt-test");
		TroilkattFS tfs = troilkatt.tfs;
		
		// Upload some data to act as a globa-meta
		String datasetsDir = OsPath.join(dataDir, "datasets");
		String[] globalFSFiles = OsPath.listdirR(datasetsDir);
		assertNotNull(globalFSFiles);		
		String hdfsGlobalMetaDir = OsPath.join(hdfsRoot, "global-meta");
		assertTrue(tfs.putLocalDirFiles(hdfsGlobalMetaDir, 1314, Utils.array2list(globalFSFiles), "tar.gz", tmpDir, tmpDir));
		
		String localfsGlobalMetaDir = OsPath.join(tmpDir, "global-meta");
		troilkatt.downloadGlobalMetaFiles(hdfsGlobalMetaDir, localfsGlobalMetaDir, tmpDir, tmpDir);
	
		assertEquals(OsPath.listdir(datasetsDir).length, OsPath.listdir(localfsGlobalMetaDir).length);
		assertTrue(OsPath.isfile(OsPath.join(localfsGlobalMetaDir, "test-pipeline.xml")));
		assertTrue(fileCmp(OsPath.join(datasetsDir, "test-pipeline.xml"), OsPath.join(localfsGlobalMetaDir, "test-pipeline.xml")));
	}
	
	@Test
	public void testListAllLeafDirs() {
		Troilkatt troilkatt = new Troilkatt();
		troilkatt.logger = Logger.getLogger("troilkatt-test");
		ArrayList<String> dirs = troilkatt.listAllLeafDirs(OsPath.join(hdfsRoot, "ls"));
		assertEquals(3, dirs.size());
		Collections.sort(dirs);
		assertEquals("subdir1", OsPath.basename(dirs.get(0)));
		assertEquals("subdir2", OsPath.basename(dirs.get(1)));
		assertEquals(OsPath.join(hdfsRoot, "ls/subdir3"), dirs.get(2));
		
		dirs = troilkatt.listAllLeafDirs(OsPath.join(hdfsRoot, "not/a/subdir"));
		assertEquals(0, dirs.size());
	}
	

	/**
	 * Test method for {@link edu.princeton.function.troilkatt.Troilkatt#run(java.lang.String[])}.
	 * @throws TroilkattPropertiesException 
	 * @throws IOException 
	 */
	@Test
	public void testRun() throws IOException, TroilkattPropertiesException {
		String datasetFile = OsPath.join(dataDir, "unitDatasets");
		String[] args1 = {"-c", OsPath.join(dataDir, configurationFile), "-d", datasetFile};
		
		Troilkatt t = new Troilkatt();
		t.run(args1);
		assertNotNull(t.status);
	}
}

