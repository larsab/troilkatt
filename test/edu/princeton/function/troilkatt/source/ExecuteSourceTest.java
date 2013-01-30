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
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;
import edu.princeton.function.troilkatt.hbase.HbaseException;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class ExecuteSourceTest extends TestSuper {
	protected static TroilkattProperties troilkattProperties;				
	protected static TroilkattHDFS tfs;
	protected static Pipeline pipeline;
	protected static Logger testLogger;
	protected static String cmd;
	
	protected static String localRootDir;
	protected static String hdfsStageMetaDir;
	protected static String hdfsStageTmpDir;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		cmd = "python " + OsPath.join(dataDir, "bin/executeSourceTest.py") + " TROILKATT.META_DIR/filelist TROILKATT.OUTPUT_DIR > TROILKATT.LOG_DIR/executeSourceTest.out 2> TROILKATT.LOG_DIR/executeSourceTest.log ";
		testLogger = Logger.getLogger("test");
		
		troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));		
		FileSystem hdfs = FileSystem.get(new Configuration());			
		tfs = new TroilkattHDFS(hdfs);
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs);
		
		localRootDir = tmpDir;
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"), OsPath.join("meta", pipeline.name));
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 0, "executeSource"));
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

	// Invalid argument
	@Test
	public void testExecuteSource() throws TroilkattPropertiesException, StageInitException {		
		ExecuteSource source = new ExecuteSource("executeSource", cmd,
				"test/executeSource", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertNotNull(source);		
		assertNotNull(source.cmd);
		assertFalse(source.cmd.contains("TROILKATT"));
	}
	
	// Invalid argument
	@Test(expected=StageInitException.class)
	public void testExecuteSource3() throws TroilkattPropertiesException, StageInitException {		
		ExecuteSource source = new ExecuteSource("executeSource", cmd + " TROILKATT.FILE",
				"test/executeSource", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertNotNull(source);		
	}
	
	// Invalid argument
	@Test(expected=StageInitException.class)
	public void testExecuteSource2() throws TroilkattPropertiesException, StageInitException {		
		ExecuteSource source = new ExecuteSource("executeSource", cmd + " TROILKATT.FILE_NOEXT",
				"test/executeSource", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertNotNull(source);
	}

	@Test
	public void testRetrieve() throws TroilkattPropertiesException, StageInitException, IOException, StageException, PipelineException, HbaseException {
		ExecuteSource source = new ExecuteSource("executeSource", cmd,
				"test/executeSource", "gz", 10, 
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
		ArrayList<String> outputFiles = source.retrieve(metaFiles, logFiles, 105);
		
		assertEquals(1, metaFiles.size());
		assertTrue(metaFiles.get(0).endsWith("filelist"));
		assertEquals(2, logFiles.size());
		Collections.sort(logFiles);
		assertTrue(logFiles.get(0).endsWith("executeSourceTest.log"));
		assertTrue(logFiles.get(1).endsWith("executeSourceTest.out"));
		assertEquals(2, outputFiles.size());		
		Collections.sort(outputFiles);
		assertTrue(outputFiles.get(0).endsWith("file1.105.gz"));
		assertTrue(outputFiles.get(1).endsWith("file2.105.gz"));
		assertTrue(tfs.isfile(OsPath.join(source.hdfsOutputDir, "file1.105.gz")));
		assertTrue(tfs.isfile(OsPath.join(source.hdfsOutputDir, "file2.105.gz")));
	}

	@Test
	public void testRetrieve2IV() throws TroilkattPropertiesException, StageInitException, IOException, StageException {
		String invalidCmd = "foobar " + cmd;

		ExecuteSource source = new ExecuteSource("executeSource", invalidCmd,
				"test/executeSource", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		ArrayList<String> logFiles = new ArrayList<String>();
		try {
			source.retrieve(new ArrayList<String>(), logFiles, 106);
			fail("StageException expected");
		} catch (StageException e) {
			// expected
		}
		
		// Check logfiles
		assertEquals(2, logFiles.size());
		Collections.sort(logFiles);
		assertTrue(logFiles.get(0).endsWith("executeSourceTest.log"));
		assertTrue(logFiles.get(1).endsWith("executeSourceTest.out"));
	}
	
	@Test
	public void testRetrieve2() throws TroilkattPropertiesException, StageInitException, IOException, StageException, PipelineException, HbaseException {
		ExecuteSource source = new ExecuteSource("executeSource", cmd,
				"test/executeSource", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		// Make sure HDFS dirs are empty
		tfs.deleteDir(source.hdfsOutputDir);
		tfs.mkdir(source.hdfsOutputDir);
		//source.logTable.schema.clearTable();
		tfs.deleteDir(source.hdfsMetaDir);
		tfs.mkdir(source.hdfsMetaDir);
		
		// Write arguments file to meta dir
		String[] files = {OsPath.join(dataDir, "files/file1"),
				OsPath.join(dataDir, "files/file2")};
		String metaFilename = OsPath.join(tmpDir, "filelist");
		FSUtils.writeTextFile(metaFilename, files);
		ArrayList<String> metaFiles = new ArrayList<String>();
		metaFiles.add(metaFilename);
		source.saveMetaFiles(metaFiles, 105);
		assertTrue(tfs.isfile(OsPath.join(source.hdfsMetaDir, "105.tar.gz")));
		
		ArrayList<String> outputFiles = source.retrieve2(107);
		assertEquals(2, outputFiles.size());		
		Collections.sort(outputFiles);
		assertTrue(outputFiles.get(0).endsWith("file1.107.gz"));
		assertTrue(outputFiles.get(1).endsWith("file2.107.gz"));
		assertTrue(tfs.isfile(OsPath.join(source.hdfsOutputDir, "file1.107.gz")));
		assertTrue(tfs.isfile(OsPath.join(source.hdfsOutputDir, "file2.107.gz")));
		assertTrue(tfs.isfile(OsPath.join(source.hdfsMetaDir, "107.tar.gz")));
		assertTrue(source.logTable.containsFile("000-executeSource", 107, "executeSourceTest.log"));
	}
}
