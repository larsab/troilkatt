package edu.princeton.function.troilkatt.pipeline;

import static org.junit.Assert.*;

import java.util.ArrayList;
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

public class NullStageTest extends TestSuper {
	protected static TroilkattProperties troilkattProperties;				
	protected static TroilkattFS tfs;
	protected static Pipeline pipeline;
	
	protected static Logger testLogger;
	
	protected static String localRootDir;
	protected static String hdfsStageMetaDir;
	protected static String hdfsStageTmpDir;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		testLogger = Logger.getLogger("test");
		
		troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));		
		FileSystem hdfs = FileSystem.get(new Configuration());			
		tfs = new TroilkattFS(hdfs);
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs);
		
		localRootDir = tmpDir;		
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"), OsPath.join("meta", pipeline.name));
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 5, "null"));
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
	public void testNullStage() throws TroilkattPropertiesException, StageInitException {
		NullStage stage = new NullStage(5, "null", "",
				null, null, 0, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertNotNull(stage);
	}

	@Test
	public void testProcess2() throws TroilkattPropertiesException, StageInitException, StageException {
		NullStage stage = new NullStage(5, "null", "",
				null, null, 0, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		ArrayList<String> inputFiles = new ArrayList<String>();
		
		inputFiles.add("foo.pcl");
		inputFiles.add("bar.dat");
		inputFiles.add("baz.pcl.none");
		inputFiles.add("bongo.mv.knn.map.avrg.pcl");
		
		ArrayList<String> outputFiles = stage.process2(inputFiles, 210);
		assertEquals(4, outputFiles.size());
	}

}
