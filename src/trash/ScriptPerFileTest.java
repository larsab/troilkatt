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

import edu.princeton.function.troilkatt.TestSuper;
import edu.princeton.function.troilkatt.Troilkatt;
import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFS;
import edu.princeton.function.troilkatt.utils.Utils;

public class ScriptPerFileTest extends TestSuper {
	protected TroilkattProperties troilkattProperties;				
	protected TroilkattFS tfs;
	
	protected static Logger testLogger;
	protected static String cmd;
	
	protected NullStage prevStage;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		cmd = "python TROILKATT.SCRIPTS/test/scriptPerFileTest.py arg1 arg2 > TROILKATT.LOG_DIR/scriptPerFileTest.out 2> TROILKATT.LOG_DIR/scriptPerFileTest.err";
		testLogger = Logger.getLogger("test");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		Troilkatt t = new Troilkatt();
		troilkattProperties = t.getProperties(OsPath.join(dataDir, configurationFile));		
		FileSystem hdfs = FileSystem.get(new Configuration());			
		tfs = new TroilkattFS(hdfs);
		prevStage = new NullStage(5, "scriptPerFile", cmd,
				"test/scriptPerFile", "gz", 10, 
				"unitPipeline", null,
				troilkattProperties, tfs);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testScriptPerDir() throws TroilkattPropertiesException, StageInitException {
		ScriptPerFile stage = new ScriptPerFile(5, "scriptPerFile", cmd,
				"test/sciptPerFile", "gz", 10, 
				"unitPipeline", prevStage,
				troilkattProperties, tfs);
		assertTrue(stage.cmd.startsWith("python"));
		
		String parts[] = stage.cmd.split(" ");
		assertEquals("-c", parts[2]);
		assertEquals("TROILKATT.INPUT_DIR/TROILKATT.FILE", parts[4]);
		assertEquals("TROILKATT.OUTPUT_DIR", parts[5]);
		assertEquals("TROILKATT.META_DIR", parts[6]);
		assertEquals("TROILKATT.GLOBALMETA_DIR", parts[7]);
		assertEquals("TROILKATT.LOG_DIR", parts[8]);
		assertEquals("arg1", parts[9]);
		assertEquals("arg2", parts[10]);
	}
	
	// Invalid argument
	@Test(expected=StageInitException.class)
	public void testScriptPerDir2() throws TroilkattPropertiesException, StageInitException {
		ScriptPerFile stage = new ScriptPerFile(5, "scriptPerFile", "",
				"test/sciptPerFile", "gz", 10, 
				"unitPipeline", prevStage,
				troilkattProperties, tfs);
		assertTrue(stage.cmd.startsWith("python"));
	}	
	
	@Test
	public void testProcess() throws TroilkattPropertiesException, StageInitException, IOException, StageException {
		ScriptPerFile stage = new ScriptPerFile(5, "scriptPerFile", cmd,
				"test/sciptPerFile", "gz", 10, 
				"unitPipeline", prevStage,
				troilkattProperties, tfs);
		
		String[] files = OsPath.listdir(OsPath.join(dataDir, "files"), testLogger);
		assertEquals(2, files.length);		
		FSUtils.writeTextFile(OsPath.join(stage.stageMetaDir, "filelist"), files);
		
		ArrayList<String> inputFiles = new ArrayList<String>();
		ArrayList<String> metaFiles = Utils.array2list(files);
		ArrayList<String> logFiles = new ArrayList<String>();
		ArrayList<String> outputFiles = stage.process(inputFiles, metaFiles, logFiles, 221);
		
		assertEquals(1, metaFiles.size());
		assertTrue(metaFiles.get(0).endsWith("filelist"));
		assertEquals(2, logFiles.size());
		Collections.sort(logFiles);
		assertTrue(logFiles.get(0).endsWith("sciptPerFile.log"));
		assertTrue(logFiles.get(1).endsWith("scriptPerFile.out"));
		assertEquals(2, outputFiles.size());		
		Collections.sort(outputFiles);
		assertTrue(outputFiles.get(0).endsWith("file1"));
		assertTrue(outputFiles.get(1).endsWith("file2"));
		assertTrue(tfs.isfile(OsPath.join(stage.hdfsOutputDir, "file1.221.gz")));
		assertTrue(tfs.isfile(OsPath.join(stage.hdfsOutputDir, "file2.221.gz")));
	}

	// Invalid command
	@Test(expected=StageException.class)
	public void testProcess2() throws TroilkattPropertiesException, StageInitException, StageException, IOException {
		ScriptPerDir stage = new ScriptPerDir(5, "scriptPerDir", "invalidCommand",
				"test/sciptPerDir", "gz", 10, 
				"unitPipeline", prevStage,
				troilkattProperties, tfs);
		
		ArrayList<String> inputFiles = new ArrayList<String>();
		ArrayList<String> metaFiles = new ArrayList<String>();
		ArrayList<String> logFiles = new ArrayList<String>();
		stage.process(inputFiles, metaFiles, logFiles, 222);
	}
}
