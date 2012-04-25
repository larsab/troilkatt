package edu.princeton.function.troilkatt.pipeline;

import static org.junit.Assert.*;

import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFS;

public class StageFactoryTest extends TestSuper {
	protected TroilkattProperties troilkattProperties;						
	protected TroilkattFS tfs;
	protected Pipeline pipeline;
	protected Logger testLogger;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {		
		troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));		
		FileSystem hdfs = FileSystem.get(new Configuration());			
		tfs = new TroilkattFS(hdfs);
		testLogger = Logger.getLogger("testLogger");
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs);
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void testNewStage() throws TroilkattPropertiesException, StageInitException {
		
		Stage stage = StageFactory.newStage("filter", 
				5, "factoryTest", "foo bar baz", 
				"test/stage", "gz", 10, 
				pipeline, testLogger);
		assertEquals(Filter.class, stage.getClass());
		
		stage = StageFactory.newStage("execute_per_dir", 
				3, "factoryTest", "foo bar baz", 
				"test/stage", "gz", 10, 
				pipeline, testLogger);
		assertEquals(ExecuteDir.class, stage.getClass());
		
		stage = StageFactory.newStage("execute_per_file", 
				4, "factoryTest", "foo bar baz", 
				"test/stage", "gz", 10, 
				pipeline, testLogger);
		assertEquals(ExecutePerFile.class, stage.getClass());
		
		stage = StageFactory.newStage("execute_per_file_mr", 
				4, "factoryTest", "foo bar baz", 
				"test/stage", "gz", 10, 
				pipeline, testLogger);
		assertEquals(ExecutePerFileMR.class, stage.getClass());
		
		stage = StageFactory.newStage("script_per_dir", 
				8, "factoryTest", "foo bar baz", 
				"test/stage", "gz", 10, 
				pipeline, testLogger);
		assertEquals(ScriptPerDir.class, stage.getClass());
		
		stage = StageFactory.newStage("script_per_file", 
				8, "factoryTest", "foo bar baz", 
				"test/stage", "gz", 10, 
				pipeline, testLogger);
		assertEquals(ScriptPerFile.class, stage.getClass());
		
		stage = StageFactory.newStage("script_per_file_mr", 
				8, "factoryTest", "foo bar baz", 
				"test/stage", "gz", 10, 
				pipeline, testLogger);
		assertEquals(ScriptPerFileMR.class, stage.getClass());
		
		stage = StageFactory.newStage("mapreduce", 
				6, "factoryTest", 
				MapReduceTest.testJar + " " + MapReduceTest.testClass + " atn1 vcsd1", 
				"test/stage", "gz", 10, 
				pipeline, testLogger);
		assertEquals(MapReduce.class, stage.getClass());

		stage = StageFactory.newStage("mapreduce_stage", 
				7, "factoryTest", 
				"execute_per_file " + MapReduceStageTest.executeCmd,
				"test/stage", "gz", 10, 
				pipeline, testLogger);
		assertEquals(MapReduceStage.class, stage.getClass());
		
		stage = StageFactory.newStage("null_stage", 
				11, "factoryTest", "foo bar baz", 
				"test/stage", "gz", 10, 
				pipeline, testLogger);
		assertEquals(NullStage.class, stage.getClass());
		
		stage = StageFactory.newStage("find_gsm_overlap", 
				8, "factoryTest", "/tmp/deleted 1 1 /tmp/list", 
				"test/stage", "gz", 10, 
				pipeline, testLogger);
		assertEquals(FindGSMOverlap.class, stage.getClass());
		
		stage = StageFactory.newStage("save_filelist", 
				8, "factoryTest", "/tmp/filelist", 
				"test/stage", "gz", 10, 
				pipeline, testLogger);
		assertEquals(SaveFilelist.class, stage.getClass());
		
		//stage = StageFactory.newStage("scipt_per_file", 
		//		9, "factoryTest", "foo bar baz", 
		//		"test/stage", "gz", 10, 
		//		pipeline,	stage,
		//		troilkattProperties, tfs, testLogger);
		//assertEquals(ScriptPerFile.class, stage.getClass());
	}
	
	@Test(expected=StageInitException.class)
	public void testNewStage2() throws TroilkattPropertiesException, StageInitException {		
		Stage stage = StageFactory.newStage("invalid_name", 
				3, "factoryTest", "foo bar baz", 
				"test/stage", "gz", 10, 
				pipeline, testLogger);
		assertEquals(ExecuteDir.class, stage.getClass());
	}

	@Test
	public void testIsValidStageName() {
		assertTrue(StageFactory.isValidStageName("execute_per_dir"));
		assertTrue(StageFactory.isValidStageName("mapreduce"));
		assertFalse(StageFactory.isValidStageName("invalid"));
	}
}
