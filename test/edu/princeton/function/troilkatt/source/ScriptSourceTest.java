package edu.princeton.function.troilkatt.source;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.PipelineException;
import edu.princeton.function.troilkatt.TestSuper;
import edu.princeton.function.troilkatt.Troilkatt;
import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFS;
import edu.princeton.function.troilkatt.hbase.HbaseException;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class ScriptSourceTest extends TestSuper {
	protected static TroilkattProperties troilkattProperties;				
	protected static TroilkattFS tfs;
	protected static Pipeline pipeline;
	
	protected static Logger testLogger;
	protected static String script;
	
	protected static String localRootDir;
	protected static String hdfsStageMetaDir;
	protected static String hdfsStageTmpDir;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		script = "TROILKATT.SCRIPTS/test/scriptSourceTest.py arg1 arg2 > TROILKATT.LOG_DIR/scriptSourceTest.out 2> TROILKATT.LOG_DIR/scriptSourceTest.err";
		testLogger = Logger.getLogger("test");
		
		troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));		
		FileSystem hdfs = FileSystem.get(new Configuration());			
		tfs = new TroilkattFS(hdfs);	
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs);
		
		localRootDir = tmpDir;
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"), OsPath.join("meta", pipeline.name));
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 0, "scriptSource"));
		hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"), "tmp");
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
	public void testScriptSource() throws IOException, TroilkattPropertiesException, StageInitException {
		ScriptSource source = new ScriptSource("scriptSource", script,
				"test/source", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertNotNull(source);
		assertTrue(source.cmd.startsWith("/usr/bin/python"));
		assertTrue(source.cmd.contains("scriptSourceTest.py"));
		assertFalse(source.cmd.contains("TROILKATT.LOG_DIR"));
		
		source = new ScriptSource("scriptSource", "/usr/bin/python " + script,
				"test/scriptSource", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertNotNull(source);
		
		source = new ScriptSource("scriptSource", "/usr/bin/python foo.py foo bar baz",
				"test/scriptSource", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertNotNull(source);
		assertTrue(source.cmd.endsWith("foo bar baz"));
	}
	
	// Invalid script
	@Test(expected=StageInitException.class)
	public void testScriptSource2() throws IOException, TroilkattPropertiesException, StageInitException {
		ScriptSource source = new ScriptSource("scriptSource", "",
				"test/scriptSource", "gz", 10,
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertNotNull(source);
	}
	
	// Invalid python bin
	@Test(expected=StageInitException.class)
	public void testScriptSource3() throws IOException, TroilkattPropertiesException, StageInitException {
		ScriptSource source = new ScriptSource("scriptSource", 
				"python " + script,
				"test/scriptSource", "gz", 10,
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertNotNull(source);
	}
	
	@Test
	public void testRetrieve() throws TroilkattPropertiesException, StageInitException, IOException, StageException, PipelineException, HbaseException {
		ScriptSource source = new ScriptSource("scriptSource", script,
				"test/scriptSource", "bz2", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		// Make sure HDFS dirs are empty
		tfs.deleteDir(source.hdfsOutputDir);
		tfs.mkdir(source.hdfsOutputDir);
		source.logTable.schema.clearTable();
		tfs.deleteDir(source.hdfsMetaDir);
		tfs.mkdir(source.hdfsMetaDir);
		
		// Write arguments file to meta dir
		String[] files = {OsPath.join(dataDir, "files/file1"),
				OsPath.join(dataDir, "files/file2")};
		String metaFilename = OsPath.join(source.stageMetaDir, "filelist");
		FSUtils.writeTextFile(metaFilename, files);
		
		ArrayList<String> metaFiles = new ArrayList<String>();
		metaFiles.add(metaFilename);
		ArrayList<String> logFiles = new ArrayList<String>();
		ArrayList<String> outputFiles = source.retrieve(metaFiles, logFiles, 107);
		
		assertEquals(1, metaFiles.size());
		assertTrue(metaFiles.get(0).endsWith("filelist"));
		assertEquals(3, logFiles.size());
		Collections.sort(logFiles);
		assertTrue(logFiles.get(0).endsWith("scriptSourceTest.err"));
		assertTrue(logFiles.get(1).endsWith("scriptSourceTest.log"));
		assertTrue(logFiles.get(2).endsWith("scriptSourceTest.out"));
		assertEquals(2, outputFiles.size());		
		Collections.sort(outputFiles);
		assertTrue(outputFiles.get(0).endsWith("file1.107.bz2"));
		assertTrue(outputFiles.get(1).endsWith("file2.107.bz2"));
		assertTrue(tfs.isfile(OsPath.join(source.hdfsOutputDir, "file1.107.bz2")));
		assertTrue(tfs.isfile(OsPath.join(source.hdfsOutputDir, "file2.107.bz2")));		
		//assertTrue(source.logTable.containsFile(0, "scriptSource", 107, "scriptSourceTest.out"));
	}
	
	// Invalid cmd
	@Test
	public void testRetrieve2() throws TroilkattPropertiesException, StageInitException, IOException, StageException {
		String invalidCmd = "notAScript.py > TROILKATT.LOG_DIR/scriptSourceTest.out 2> TROILKATT.LOG_DIR/scriptSourceTest.err";

		ScriptSource source = new ScriptSource("unitSource", invalidCmd,
				"test/scriptSource", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		ArrayList<String> logFiles = new ArrayList<String>();
		try {
			source.retrieve(new ArrayList<String>(), logFiles, 108);
			fail("StageException expected");
		} catch (StageException e) {
			// expected
		}
		
		// Check logfiles
		assertEquals(2, logFiles.size());
		Collections.sort(logFiles);
		assertTrue(logFiles.get(0).endsWith("scriptSourceTest.err"));
		assertTrue(logFiles.get(1).endsWith("scriptSourceTest.out"));
	}
}
