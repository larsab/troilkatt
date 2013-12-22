package edu.princeton.function.troilkatt.pipeline;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
import edu.princeton.function.troilkatt.fs.LogTableHbase;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;
import edu.princeton.function.troilkatt.hbase.HbaseException;

public class ExecuteDirTest extends TestSuper {
	protected static TroilkattProperties troilkattProperties;				
	protected static TroilkattHDFS tfs;
	protected static LogTableHbase lt;
	protected static Pipeline pipeline;
	
	protected static Logger testLogger;
	protected static String cmd;
	
	protected static String localRootDir;
	protected static String hdfsStageMetaDir;
	protected static String hdfsStageTmpDir;
	
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		cmd = "python " + OsPath.join(dataDir, "bin/executeDirTest.py") + " TROILKATT.INPUT_DIR TROILKATT.OUTPUT_DIR TROILKATT.META_DIR/filelist > TROILKATT.LOG_DIR/executeDirTest.out 2> TROILKATT.LOG_DIR/executeDirTest.log";
		testLogger = Logger.getLogger("test");
		
		troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));		
		FileSystem hdfs = FileSystem.get(new Configuration());
		tfs = new TroilkattHDFS(hdfs);
		lt = new LogTableHbase("unitPipeline", HBaseConfiguration.create());
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs, lt);
		
		initTestDir();
		
		localRootDir = tmpDir;		
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), OsPath.join("meta", pipeline.name));
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 5, "executeDir"));
		hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "tmp");
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
	public void testExecuteDir() throws TroilkattPropertiesException, StageInitException {
		ExecuteDir stage = new ExecuteDir(5, "executeDir", cmd,
				"test/executeDir", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertNotNull(stage.cmd);
		assertFalse(stage.cmd.contains("TROILKATT"));
	}

	// Invalid argument
	@Test(expected=StageInitException.class)
	public void testExecuteDir2() throws TroilkattPropertiesException, StageInitException {
		ExecuteDir stage = new ExecuteDir(5, "executeDir", cmd + " TROILKATT.FILE",
				"test/executeDir", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertEquals(stage.cmd, cmd);
	}
	
	// Invalid argument
	@Test(expected=StageInitException.class)
	public void testExecuteDir3() throws TroilkattPropertiesException, StageInitException {
		ExecuteDir stage = new ExecuteDir(5, "executeDir", cmd + " TROILKATT.FILE_NOEXT",
				"test/executeDir", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertEquals(stage.cmd, cmd);
	}
	
	@Test
	public void testProcess() throws TroilkattPropertiesException, StageInitException, StageException, IOException {
		ExecuteDir stage = new ExecuteDir(5, "executeDir", cmd,
				"test/executeDir", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		assertTrue(OsPath.copy(OsPath.join(dataDir, "files/file1"), 
				OsPath.join(stage.stageInputDir, "file1")));
		assertTrue(OsPath.copy(OsPath.join(dataDir, "files/file2"), 
				OsPath.join(stage.stageInputDir, "file2")));
		
		String[] files = OsPath.listdir(stage.stageInputDir, testLogger);
		assertEquals(2, files.length);		
		String metaFilename = OsPath.join(stage.stageMetaDir, "filelist");
		FSUtils.writeTextFile(metaFilename, files);
		
		ArrayList<String> inputFiles = new ArrayList<String>();
		ArrayList<String> metaFiles = new ArrayList<String>();
		metaFiles.add(metaFilename);
		ArrayList<String> logFiles = new ArrayList<String>();
		ArrayList<String> outputFiles = stage.process(inputFiles, metaFiles, logFiles, 201);
		
		assertEquals(1, metaFiles.size());
		assertTrue(metaFiles.get(0).endsWith("filelist"));
		assertEquals(2, logFiles.size());
		Collections.sort(logFiles);
		assertTrue(logFiles.get(0).endsWith("executeDirTest.log"));
		assertTrue(logFiles.get(1).endsWith("executeDirTest.out"));
		assertEquals(2, outputFiles.size());		
		Collections.sort(outputFiles);
		assertTrue(outputFiles.get(0).endsWith("file1"));
		assertTrue(outputFiles.get(1).endsWith("file2"));
	}

	// Invalid command
	@Test
	public void testProcess2I() throws TroilkattPropertiesException, StageInitException, StageException, IOException {
		String invalidCmd = "python " + OsPath.join(dataDir, "bin/executeDirTest.py") + " > TROILKATT.LOG_DIR/executeDirTest.out 2> TROILKATT.LOG_DIR/executeDirTest.log";
		
		ExecuteDir stage = new ExecuteDir(5, "executeDir", invalidCmd,
				"test/executeDir", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		ArrayList<String> inputFiles = new ArrayList<String>();
		ArrayList<String> metaFiles = new ArrayList<String>();
		ArrayList<String> logFiles = new ArrayList<String>();
		try {
			stage.process(inputFiles, metaFiles, logFiles, 202);
			fail("StageException should have been thrown");
		} catch (StageException e) {
			// expected
		}
		
		// log files should still have been created
		assertEquals(2, logFiles.size());
		Collections.sort(logFiles);
		assertTrue(logFiles.get(0).endsWith("executeDirTest.log"));
		assertTrue(logFiles.get(1).endsWith("executeDirTest.out"));
	}
	
	// Process 2
	@Test
	public void testProcess2() throws TroilkattPropertiesException, StageInitException, StageException, IOException, PipelineException, HbaseException {
		ExecuteDir stage = new ExecuteDir(5, "executeDir", cmd,
				"test/executeDir", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		// Make sure HDFS dirs are empty
		tfs.deleteDir(stage.tfsOutputDir);
		tfs.mkdir(stage.tfsOutputDir);	
		//stage.logTable.schema.clearTable();
		tfs.deleteDir(stage.tfsMetaDir);
		tfs.mkdir(stage.tfsMetaDir);
		
		ArrayList<String> inputFiles = tfs.listdirR("troilkatt/data/test/input");
		assertEquals(6, inputFiles.size());		
		String[] files = new String[inputFiles.size()];
		for (int i = 0; i < files.length; i++) {
			files[i] = OsPath.join(stage.stageInputDir, tfs.getFilenameName(inputFiles.get(i)));
		}
		
		String metaFilename = OsPath.join(stage.stageMetaDir, "filelist");
		FSUtils.writeTextFile(metaFilename, files);
		ArrayList<String> metaFiles = new ArrayList<String>();
		metaFiles.add(metaFilename);
		stage.saveMetaFiles(metaFiles, 202);
		OsPath.delete(metaFilename);
		
		//long timestamp = TroilkattStatus.getTimestamp();
		ArrayList<String> outputFiles = stage.process2(inputFiles, 202);
		
		assertEquals(6, outputFiles.size());
		Collections.sort(outputFiles);
		assertTrue(outputFiles.get(0).endsWith("file1.202.gz"));
		assertTrue(outputFiles.get(1).endsWith("file2.202.gz"));
		assertTrue(outputFiles.get(5).endsWith("file6.202.gz"));
		assertTrue(tfs.isfile(OsPath.join(stage.tfsOutputDir, "file1.202.gz")));
		assertTrue(tfs.isfile(OsPath.join(stage.tfsOutputDir, "file2.202.gz")));
		assertTrue(tfs.isfile(OsPath.join(stage.tfsOutputDir, "file6.202.gz")));
		assertTrue(tfs.isfile(OsPath.join(stage.tfsMetaDir, "202.tar.gz")));
		assertTrue(stage.logTable.containsFile(stage.stageName, 202, "executeDirTest.log"));	
	}
}
