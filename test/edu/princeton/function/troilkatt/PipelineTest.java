package edu.princeton.function.troilkatt;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.princeton.function.troilkatt.pipeline.NullStage;
import edu.princeton.function.troilkatt.pipeline.StageInitException;
import edu.princeton.function.troilkatt.sink.NullSink;
import edu.princeton.function.troilkatt.source.NullSource;
import edu.princeton.function.troilkatt.fs.LogTableHbase;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFS;

/**
 * These tests requires a working hadoop configuration accesible from the hosts the tests are run on.
 * 
 * The easiest way to run the tests is therefore to install a pseudo-mode hadoop installation on localhost, 
 * or create a ssh tunnel for the NameNode and DataNode port of a pseudo-mode installation running in another
 * host. Note that in order to use a distributed hadoop instance, the node where the tests are run must be
 * able to connect to all DataNodes. 
 */

public class PipelineTest extends TestSuper {
	static protected Logger testLogger;
	
	protected Troilkatt troilkatt;
	protected TroilkattProperties troilkattProperties;
	protected TroilkattFS tfs;
	protected LogTableHbase lt;
	
	private String pipelineName = "pipelineTest";
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		testLogger = Logger.getLogger("testLogger");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		troilkatt = new Troilkatt();
		troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));
		tfs = troilkatt.setupTFS(troilkattProperties);
		lt = new LogTableHbase(pipelineName, HBaseConfiguration.create());
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testPipeline() throws IOException, PipelineException, TroilkattPropertiesException, StageInitException {		
		String pipelineFile = OsPath.join(dataDir, "datasets/test-pipeline.xml");
		Pipeline p = new Pipeline(pipelineName, pipelineFile, 
				troilkattProperties, tfs, lt);
		assertNotNull(p);
		assertEquals(pipelineName, p.name);
		assertEquals(troilkattProperties, p.troilkattProperties);		
		assertNotNull(p.logTable);
		String localRoot = troilkattProperties.get("troilkatt.localfs.dir");
		assertTrue(OsPath.isdir(OsPath.join(localRoot, pipelineName)));
		String hdfsRoot = troilkattProperties.get("troilkatt.tfs.root.dir");
		assertTrue(tfs.isdir(OsPath.join(hdfsRoot, "data")));
		assertTrue(tfs.isdir(OsPath.join(hdfsRoot, "log/" + pipelineName)));
		assertTrue(tfs.isdir(OsPath.join(hdfsRoot, "meta/" + pipelineName)));
		assertTrue(tfs.isdir(OsPath.join(hdfsRoot, "global-meta")));		
		verifyPipeline(p, troilkattProperties);
	}
	
	/**
	 * Verify testPipeline object
	 * @throws TroilkattPropertiesException 
	 */
	public static void verifyPipeline(Pipeline p, TroilkattProperties troilkattProperties) throws TroilkattPropertiesException {
		assertNotNull(p.source);
		assertNotNull(p.sink);
		assertEquals(3, p.pipeline.size());
		
		// Test source arguments
		assertEquals("000-theSource", p.source.stageName);
		assertEquals(NullSource.class, p.source.getClass());
		assertEquals("arg1 arg2 arg3", p.source.args);
		String hdfsRoot = troilkattProperties.get("troilkatt.tfs.root.dir");
		assertEquals(OsPath.join(hdfsRoot, "data/test/crawler-output"), p.source.tfsOutputDir);
		assertEquals("gz", p.source.compressionFormat);
		assertEquals(-1, p.source.storageTime);
		
		// Test stages
		assertEquals("001-firstStage", p.pipeline.get(0).stageName);
		assertEquals(NullStage.class, p.pipeline.get(0).getClass());
		assertEquals("arg1 arg2", p.pipeline.get(0).args);
		assertEquals(OsPath.join(hdfsRoot, "data/test/stage1-output"), p.pipeline.get(0).tfsOutputDir);
		assertEquals("none", p.pipeline.get(0).compressionFormat);
		assertEquals(1, p.pipeline.get(0).storageTime);
		
		assertEquals("002-secondStage", p.pipeline.get(1).stageName);
		assertEquals(NullStage.class, p.pipeline.get(1).getClass());
		assertEquals("arg1 arg2 arg3 arg4", p.pipeline.get(1).args);		
		assertNull(p.pipeline.get(1).tfsOutputDir);
				
		assertEquals("003-thirdStage", p.pipeline.get(2).stageName);
		assertEquals(NullStage.class, p.pipeline.get(2).getClass());
		assertEquals("", p.pipeline.get(2).args);		
		assertNotNull(p.pipeline.get(2).tfsOutputDir);
		assertEquals("bz2", p.pipeline.get(2).compressionFormat);
		assertEquals(2, p.pipeline.get(2).storageTime);
		
		// Test sink
		assertEquals("004-theSink", p.sink.stageName);
		assertEquals(NullSink.class, p.sink.getClass());
		assertEquals("arg1", p.sink.args);
	}
	
	// With invalid pipeline file
	@Test(expected=PipelineException.class)
	public void testPipeline2() throws IOException, PipelineException, TroilkattPropertiesException, StageInitException {		
		String pipelineFile = OsPath.join(dataDir, "datasets/invalid-pipeline1.xml");
		assertNotNull( new Pipeline(pipelineName, pipelineFile, 
				troilkattProperties, tfs, lt) );
	}
	
	// With invalid pipeline file
	@Test(expected=PipelineException.class)
	public void testPipeline3() throws IOException, PipelineException, TroilkattPropertiesException, StageInitException {		
		String pipelineFile = OsPath.join(dataDir, "datasets/invalid-pipeline2.xml");
		assertNotNull( new Pipeline(pipelineName, pipelineFile, 
				troilkattProperties, tfs, lt) );
	}
	
	// With invalid pipeline file
	@Test(expected=PipelineException.class)
	public void testPipeline4() throws IOException, PipelineException, TroilkattPropertiesException, StageInitException {		
		String pipelineFile = OsPath.join(dataDir, "datasets/invalid-pipeline3.xml");
		assertNotNull( new Pipeline(pipelineName, pipelineFile, 
				troilkattProperties, tfs, lt) );
	}
	
	// With invalid pipeline file
	@Test(expected=StageInitException.class)
	public void testPipeline5() throws IOException, PipelineException, TroilkattPropertiesException, StageInitException {		
		String pipelineFile = OsPath.join(dataDir, "datasets/invalid-pipeline4.xml");
		assertNotNull( new Pipeline(pipelineName, pipelineFile, 
				troilkattProperties, tfs, lt) );
	}
	
	// With invalid pipeline file
	@Test(expected=PipelineException.class)
	public void testPipeline6() throws IOException, PipelineException, TroilkattPropertiesException, StageInitException {		
		String pipelineFile = OsPath.join(dataDir, "datasets/invalid-pipeline5.xml");
		assertNotNull( new Pipeline(pipelineName, pipelineFile, 
				troilkattProperties, tfs, lt) );
	}
	
	// With invalid pipeline file
	@Test(expected=PipelineException.class)
	public void testPipeline7() throws IOException, PipelineException, TroilkattPropertiesException, StageInitException {		
		String pipelineFile = OsPath.join(dataDir, "datasets/invalid-pipeline6.xml");
		assertNotNull( new Pipeline(pipelineName, pipelineFile, 
				troilkattProperties, tfs, lt) );
	}
		
	// With valid pipeline file
	@Test
	public void testPipeline9() throws IOException, PipelineException, TroilkattPropertiesException, StageInitException {		
		String pipelineFile = OsPath.join(dataDir, "datasets/valid-pipeline1.xml");
		assertNotNull( new Pipeline(pipelineName, pipelineFile, 
				troilkattProperties, tfs, lt) );
	}

	// With invalid pipeline file
	@Test(expected=StageInitException.class)
	public void testPipeline10() throws IOException, PipelineException, TroilkattPropertiesException, StageInitException {		
		String pipelineFile = OsPath.join(dataDir, "datasets/invalid-pipeline8.xml");
		assertNotNull( new Pipeline(pipelineName, pipelineFile, 
				troilkattProperties, tfs, lt) );
	}
	
	// With invalid pipeline file
	@Test(expected=StageInitException.class)
	public void testPipeline11() throws IOException, PipelineException, TroilkattPropertiesException, StageInitException {		
		String pipelineFile = OsPath.join(dataDir, "datasets/invalid-pipeline9.xml");
		assertNotNull( new Pipeline(pipelineName, pipelineFile, 
				troilkattProperties, tfs, lt) );
	}
	
	// With invalid pipeline file
	@Test(expected=StageInitException.class)
	public void testPipeline12() throws IOException, PipelineException, TroilkattPropertiesException, StageInitException {		
		String pipelineFile = OsPath.join(dataDir, "datasets/invalid-pipeline10.xml");
		assertNotNull( new Pipeline(pipelineName, pipelineFile, 
				troilkattProperties, tfs, lt) );
	}
	
	// Not unit tested
	//@Test
	//public void testUpdate() {
	//}

	// Not unit tested
	//@Test
	//public void testRecover() {
	//}

	// Alternative constructor: tested in PipelinePlaceholderTest
	//@Test
	//public void testPipeline13() throws IOException, PipelineException, TroilkattPropertiesException, StageInitException {
	//}

	@Test
	public void testOpenPipeline() throws PipelineException, IOException, TroilkattPropertiesException {
		String pipelineFile = OsPath.join(dataDir, "datasets/test-pipeline.xml");
		Pipeline p = Pipeline.openPipeline(pipelineFile, troilkattProperties, tfs, lt, testLogger);
		verifyPipeline(p, troilkattProperties);
	}
	
	// Invalid datasetname
	@Test(expected=PipelineException.class)
	public void testOpenPipeline2() throws PipelineException, IOException, TroilkattPropertiesException {
		String pipelineFile = OsPath.join(dataDir, "datasets/test-pipeline");
		Pipeline.openPipeline(pipelineFile, troilkattProperties, tfs, lt, testLogger);
	}
	
	// Invalid dataset
	@Test(expected=PipelineException.class)
	public void testOpenPipeline3() throws PipelineException, IOException, TroilkattPropertiesException {
		String pipelineFile = OsPath.join(dataDir, "datasets/invalid-pipeline1.xml");
		Pipeline.openPipeline(pipelineFile, troilkattProperties, tfs, lt, testLogger);
	}

	
	// Not unit tested
	//@Test
	//public void testCleanup() throws PipelineException, TroilkattPropertiesException {
	//	String pipelineFile = OsPath.join(dataDir, "datasets/test-pipeline.xml");
	//	Pipeline p = Pipeline.openPipeline(pipelineFile, troilkattProperties, tfs, testLogger);
	//	verifyPipeline(p);
	//}
}
