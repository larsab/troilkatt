package edu.princeton.function.troilkatt.pipeline;

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
import edu.princeton.function.troilkatt.utils.Utils;

public class ExecutePerFileTest extends TestSuper {
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
		cmd = "python " + OsPath.join(dataDir, "bin/executePerFileTest.py") + " TROILKATT.INPUT_DIR/TROILKATT.FILE TROILKATT.OUTPUT_DIR/TROILKATT.FILE_NOEXT.out TROILKATT.META_DIR/filelist > TROILKATT.LOG_DIR/executePerFileTest.out 2> TROILKATT.LOG_DIR/executePerFileTest.log";
		testLogger = Logger.getLogger("test");
		
		troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));		
		FileSystem hdfs = FileSystem.get(new Configuration());			
		tfs = new TroilkattHDFS(hdfs);
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs);
		
		localRootDir = tmpDir;		
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), OsPath.join("meta", pipeline.name));
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 5, "executePerFile"));
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
	public void testExecutePerFile() throws TroilkattPropertiesException, StageInitException {
		ExecutePerFile stage = new ExecutePerFile(5, "executePerFile", cmd,
				"test/executePerFile", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertNotNull(stage.cmd);
		assertTrue(stage.cmd.contains("TROILKATT.FILE"));
		assertTrue(stage.cmd.contains("TROILKATT.FILE_NOEXT"));
		assertFalse(stage.cmd.contains("TROILKATT.INPUT_DIR"));
	}

	@Test
	public void testProcess() throws IOException, TroilkattPropertiesException, StageInitException, StageException {
		ExecutePerFile stage = new ExecutePerFile(5, "executePerFile", cmd,
				"test/executePerFile", "gz", 10, 
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
		
		ArrayList<String> inputFiles = Utils.array2list(files);
		ArrayList<String> metaFiles = new ArrayList<String>();
		metaFiles.add(metaFilename);
		ArrayList<String> logFiles = new ArrayList<String>();
		ArrayList<String> outputFiles = stage.process(inputFiles, metaFiles, logFiles, 203);
		
		assertEquals(1, metaFiles.size());
		assertTrue(metaFiles.get(0).endsWith("filelist"));
		assertEquals(2, logFiles.size());
		Collections.sort(logFiles);
		assertTrue(logFiles.get(0).endsWith("executePerFileTest.log"));
		assertTrue(logFiles.get(1).endsWith("executePerFileTest.out"));
		assertEquals(2, outputFiles.size());		
		Collections.sort(outputFiles);
		assertTrue(outputFiles.get(0).endsWith("file1.out"));
		assertTrue(outputFiles.get(1).endsWith("file2.out"));		
	}

	// Invalid file
	@Test
	public void testProcess2I() throws TroilkattPropertiesException, StageInitException, StageException, IOException {
		ExecutePerFile stage = new ExecutePerFile(5, "executePerFile", cmd,
				"test/executePerFile", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		String[] files = OsPath.listdir(OsPath.join(dataDir, "files"), testLogger);
		FSUtils.writeTextFile(OsPath.join(stage.stageMetaDir, "filelist"), files);
		
		ArrayList<String> inputFiles = Utils.array2list(files); 
		inputFiles.add("vfdgsdfg2");
		ArrayList<String> metaFiles = Utils.array2list(files);
		ArrayList<String> logFiles = new ArrayList<String>();
		try {
			stage.process(inputFiles, metaFiles, logFiles, 203);
			fail("StageException should have been thrown");
		} catch (StageException e) {
			// expected
		}
		
		// Log files should still be produced
		assertEquals(2, logFiles.size());
		Collections.sort(logFiles);
		assertTrue(logFiles.get(0).endsWith("executePerFileTest.log"));
		assertTrue(logFiles.get(1).endsWith("executePerFileTest.out"));
	}

	// Invalid command
	@Test
	public void testProcess3I() throws TroilkattPropertiesException, StageInitException, StageException {
		ExecutePerFile stage = new ExecutePerFile(5, "executePerFile", "boing vopg > TROILKATT.LOG_DIR/executePerFileTest.out 2> TROILKATT.LOG_DIR/executePerFileTest.log",
				"test/executePerFile", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		assertTrue(OsPath.copy(OsPath.join(dataDir, "files/file1"), 
				OsPath.join(stage.stageInputDir, "file1")));
		assertTrue(OsPath.copy(OsPath.join(dataDir, "files/file2"), 
				OsPath.join(stage.stageInputDir, "file2")));
		
		String[] files = OsPath.listdir(stage.stageInputDir, testLogger);
		ArrayList<String> inputFiles = Utils.array2list(files);
		
		ArrayList<String> metaFiles = new ArrayList<String>();
		ArrayList<String> logFiles = new ArrayList<String>();
		
		try {
			stage.process(inputFiles, metaFiles, logFiles, 204);
			fail("StageException should have been thrown");
		} catch (StageException e) {
			// expected
		}
		
		// Log files should still be produced
		assertEquals(2, logFiles.size());
		Collections.sort(logFiles);
		assertTrue(logFiles.get(0).endsWith("executePerFileTest.log"));
		assertTrue(logFiles.get(1).endsWith("executePerFileTest.out"));
	}
	
	@Test
	public void testProcess2() throws IOException, TroilkattPropertiesException, StageInitException, StageException, PipelineException, HbaseException {
		ExecutePerFile stage = new ExecutePerFile(5, "executePerFile", cmd,
				"test/executePerFile", "gz", 10, 				
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
			files[i] = tfs.getFilenameName(inputFiles.get(i));
		}
		String metaFilename = OsPath.join(stage.stageMetaDir, "filelist");
		FSUtils.writeTextFile(metaFilename, files);
		
		ArrayList<String> outputFiles = stage.process2(inputFiles, 205);
		Collections.sort(outputFiles);
		
		assertEquals(6, outputFiles.size());
		assertTrue(outputFiles.get(0).endsWith("file1.out.205.gz"));
		assertTrue(outputFiles.get(1).endsWith("file2.out.205.gz"));
		assertTrue(outputFiles.get(5).endsWith("file6.out.205.gz"));
		assertTrue(tfs.isfile(OsPath.join(stage.tfsOutputDir, "file1.out.205.gz")));
		assertTrue(tfs.isfile(OsPath.join(stage.tfsOutputDir, "file2.out.205.gz")));
		assertTrue(tfs.isfile(OsPath.join(stage.tfsOutputDir, "file6.out.205.gz")));
		assertTrue(tfs.isfile(OsPath.join(stage.tfsMetaDir, "205.tar.gz")));
		assertTrue(stage.logTable.containsFile(stage.stageName, 205, "executePerFileTest.log"));			
	}
}
