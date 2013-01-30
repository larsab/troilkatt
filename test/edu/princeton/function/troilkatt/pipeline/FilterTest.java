package edu.princeton.function.troilkatt.pipeline;

import static org.junit.Assert.*;

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
import edu.princeton.function.troilkatt.TestSuper;
import edu.princeton.function.troilkatt.Troilkatt;
import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;

public class FilterTest extends TestSuper {
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
		cmd = "python " + OsPath.join(dataDir, "bin/executeDirTest.py") + "TROILKATT.INPUT_DIR TROILKATT.OUTPUT_DIR TROILKATT.META_DIR/filelist > TROILKATT.LOG_DIR/executeSourceTest.out 2> TROILKATT.LOG_DIR/executeSoruceTest.log ";
		testLogger = Logger.getLogger("test");
		
		troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));		
		FileSystem hdfs = FileSystem.get(new Configuration());			
		tfs = new TroilkattHDFS(hdfs);
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs);
		
		localRootDir = tmpDir;		
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"), OsPath.join("meta", pipeline.name));
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 5, "filter"));
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
	public void testFilter() throws TroilkattPropertiesException, StageInitException {
		Filter stage = new Filter(5, "filter", ".*\\.pcl",
				null, null, 0, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertNotNull(stage.pattern);
	}
	
	// Invalid pattern
	@Test(expected=StageInitException.class)
	public void testFilter2() throws TroilkattPropertiesException, StageInitException {
		Filter stage = new Filter(5, "filter", "[foo",
				null, null, 0, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertNotNull(stage.pattern);
	}

	// process2 is not unit tested
	//@Test
	//public void testProcess2() {
	//}

	@Test
	public void testProcess() throws TroilkattPropertiesException, StageInitException, StageException {
		Filter stage = new Filter(5, "filter", ".*\\.pcl",
				null, null, 0, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		ArrayList<String> inputFiles = new ArrayList<String>();
		ArrayList<String> metaFiles = new ArrayList<String>();
		ArrayList<String> logFiles = new ArrayList<String>();
		
		inputFiles.add("foo.pcl");
		inputFiles.add("bar.dat");
		inputFiles.add("baz.pcl.none");
		inputFiles.add("bongo.mv.knn.map.avrg.pcl");
		
		ArrayList<String> outputFiles = stage.process(inputFiles, metaFiles, logFiles, 210);
		assertEquals(3, outputFiles.size());
		Collections.sort(outputFiles);
		assertEquals("baz.pcl.none", outputFiles.get(0));
		assertEquals("bongo.mv.knn.map.avrg.pcl", outputFiles.get(1));
		assertEquals("foo.pcl", outputFiles.get(2));
	}

}
