package edu.princeton.function.troilkatt.pipeline;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TestSuper;
import edu.princeton.function.troilkatt.Troilkatt;
import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;
import edu.princeton.function.troilkatt.utils.Utils;

public class ScriptPerFileTest extends TestSuper {
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
		cmd = "/usr/bin/python TROILKATT.SCRIPTS/test/scriptPerFileTest.py arg1 arg2 > TROILKATT.LOG_DIR/scriptPerFileTest-TROILKATT.FILE.out 2> TROILKATT.LOG_DIR/scriptPerFileTest-TROILKATT.FILE.err";
		testLogger = Logger.getLogger("test");

		troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));		
		FileSystem hdfs = FileSystem.get(new Configuration());			
		tfs = new TroilkattHDFS(hdfs);
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs);

		localRootDir = tmpDir;		
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), OsPath.join("meta", pipeline.name));
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 5, "scriptPerDir"));
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
	public void testScriptPerFile() throws TroilkattPropertiesException, StageInitException {
		ScriptPerFile stage = new ScriptPerFile(5, "scriptPerFile", cmd,
				"test/scriptPerFile", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertTrue(stage.cmd.startsWith("/usr/bin/python"));

		String parts[] = stage.cmd.split(" ");
		assertEquals("-c", parts[2]);
		assertEquals("-l", parts[4]);
		assertEquals("-t", parts[6]);
		assertEquals("TROILKATT.TIMESTAMP", parts[7]);
		assertEquals("-f", parts[8]);
		assertEquals("TROILKATT.FILE", parts[9]);
		assertEquals(stage.stageInputDir, parts[10]);
		assertEquals(stage.stageOutputDir, parts[11]);
		assertEquals(stage.stageMetaDir, parts[12]);
		assertEquals(stage.stageLogDir, parts[13]);
		assertEquals(stage.stageTmpDir, parts[14]);
		assertEquals("arg1", parts[15]);
		assertEquals("arg2", parts[16]);
	}

	// Invalid command
	@Test(expected=StageInitException.class)
	public void testScriptPerFile2() throws TroilkattPropertiesException, StageInitException {
		ScriptPerFile stage = new ScriptPerFile(5, "scriptPerFile", "",
				"test/scriptPerFile", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertTrue(stage.cmd.startsWith("python"));
	}	


	// Invalid python binary
	@Test(expected=StageInitException.class)
	public void testScriptPerFile3() throws TroilkattPropertiesException, StageInitException {
		ScriptPerFile stage = new ScriptPerFile(5, "scriptPerFile", cmd.replace("/usr/bin/python", "python"),
				"test/scriptPerFile", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertNull(stage);
	}

	@Test
	public void testProcess() throws TroilkattPropertiesException, StageInitException, IOException, StageException {
		ScriptPerFile stage = new ScriptPerFile(5, "ScriptPerFile", cmd,
				"test/ScriptPerFile", "gz", 10, 
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
		ArrayList<String> outputFiles = stage.process(inputFiles, metaFiles, logFiles, 211);

		assertEquals(1, metaFiles.size());
		assertTrue(metaFiles.get(0).endsWith("filelist"));
		assertEquals(6, logFiles.size());
		Collections.sort(logFiles);
		assertTrue(logFiles.get(0).endsWith("scriptPerFileTest-file1.err"));
		assertTrue(logFiles.get(1).endsWith("scriptPerFileTest-file1.log"));
		assertTrue(logFiles.get(2).endsWith("scriptPerFileTest-file1.out"));
		assertTrue(logFiles.get(3).endsWith("scriptPerFileTest-file2.err"));
		assertTrue(logFiles.get(4).endsWith("scriptPerFileTest-file2.log"));
		assertTrue(logFiles.get(5).endsWith("scriptPerFileTest-file2.out"));
		assertEquals(2, outputFiles.size());		
		Collections.sort(outputFiles);
		assertTrue(outputFiles.get(0).endsWith("file1"));
		assertTrue(outputFiles.get(1).endsWith("file2"));
	}
	
	// Invalid meta
	@Test(expected=StageException.class)
	public void testProcess3() throws TroilkattPropertiesException, StageInitException, IOException, StageException {
		ScriptPerFile stage = new ScriptPerFile(5, "ScriptPerFile", cmd,
				"test/ScriptPerFile", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);

		assertTrue(OsPath.copy(OsPath.join(dataDir, "files/file1"), 
				OsPath.join(stage.stageInputDir, "file1")));

		String[] files = OsPath.listdir(stage.stageInputDir, testLogger);
		assertEquals(1, files.length);		
		String metaFilename = OsPath.join(stage.stageMetaDir, "filelist");
		FSUtils.writeTextFile(metaFilename, files);
		
		// File2 is not in meta file so the script should exit with -1
		assertTrue(OsPath.copy(OsPath.join(dataDir, "files/file2"), 
				OsPath.join(stage.stageInputDir, "file2")));
		files = OsPath.listdir(stage.stageInputDir, testLogger);
	
		ArrayList<String> inputFiles = Utils.array2list(files);
		ArrayList<String> metaFiles = new ArrayList<String>();
		metaFiles.add(metaFilename);
		ArrayList<String> logFiles = new ArrayList<String>();
		stage.process(inputFiles, metaFiles, logFiles, 213);		
	}

	// Invalid command
	@Test
	public void testProcessI() throws TroilkattPropertiesException, StageInitException, StageException, IOException {
		ScriptPerFile stage = new ScriptPerFile(5, "ScriptPerFile", "invalidCommand > TROILKATT.LOG_DIR/scriptPerDirTest.out 2> TROILKATT.LOG_DIR/scriptPerDirTest.err",
				"test/ScriptPerFile", "gz", 10, 
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
		
		try {
			stage.process(inputFiles, metaFiles, logFiles, 212);
			fail("StageException expected");
		} catch (StageException e) {
			// expected
		}

		// check logfiles
		Collections.sort(logFiles);
		assertTrue(logFiles.get(0).endsWith("scriptPerDirTest.err"));		
		assertTrue(logFiles.get(1).endsWith("scriptPerDirTest.out"));
	}
}
