package edu.princeton.function.troilkatt.source;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.log4j.Logger;

import org.apache.hadoop.hbase.HBaseConfiguration;
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
import edu.princeton.function.troilkatt.fs.LogTableHbase;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFS;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class SourceFactoryTest extends TestSuper {
	protected TroilkattProperties troilkattProperties;						
	protected TroilkattFS tfs;
	protected LogTableHbase lt;
	protected Pipeline pipeline;
	protected Logger testLogger;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TestSuper.initTestDir();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {	
		Troilkatt troilkatt = new Troilkatt();
		troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));				
		tfs = troilkatt.setupTFS(troilkattProperties);
		testLogger = Logger.getLogger("testLogger");
		lt = new LogTableHbase("unitPipeline", HBaseConfiguration.create());
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs, lt);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testNewSource() throws IOException, TroilkattPropertiesException, StageInitException {				
		Source source = SourceFactory.newSource("execute_source", 
				"factoryTest", "foo bar baz", "test/source", "gz", 10, 
				pipeline, testLogger);
		assertEquals(ExecuteSource.class, source.getClass());
		assertFalse(NullSource.class == source.getClass());
		source = SourceFactory.newSource("file_source", 
				"factoryTest", OsPath.join(dataDir, configurationFile), "test/source", "gz", 10, 
				pipeline, testLogger);
		assertEquals(FileSource.class, source.getClass());
		source = SourceFactory.newSource("geo_gds_mirror", 
				"factoryTest", "foo bar baz", "test/source", "gz", 10, 
				pipeline, testLogger);
		assertEquals(GeoGDSMirror.class, source.getClass());
		source = SourceFactory.newSource("geo_gse_mirror", 
				"factoryTest", "foo bar baz", "test/source", "gz", 10, 
				pipeline, testLogger);
		assertEquals(GeoGSEMirror.class, source.getClass());
		source = SourceFactory.newSource("geo_raw_mirror", 
				"factoryTest", "foo bar baz", "test/source", "gz", 10, 
				pipeline, testLogger);
		assertEquals(GeoRawMirror.class, source.getClass());
		
		source = SourceFactory.newSource("geo_raw_org", 
				"factoryTest", "foo bar baz", "test/source", "gz", 10, 
				pipeline, testLogger);
		assertEquals(GeoRawOrg.class, source.getClass());
		
		source = SourceFactory.newSource("list_dir", 
				"factoryTest",  OsPath.join(hdfsRoot, "ls"), "test/source", "gz", 10, 
				pipeline, testLogger);
		assertEquals(ListDir.class, source.getClass());		
		source = SourceFactory.newSource("list_dir_new", 
				"factoryTest",  OsPath.join(hdfsRoot, "ls"), "test/source", "gz", 10, 
				pipeline, testLogger);
		assertEquals(ListDirNew.class, source.getClass());
		source = SourceFactory.newSource("list_dir_diff", 
				"factoryTest",  OsPath.join(hdfsRoot, "ls") + " " +  OsPath.join(hdfsRoot, "ls"), "test/source", "gz", 10, 
				pipeline, testLogger);				
		assertEquals(ListDirDiff.class, source.getClass());
		
		source = SourceFactory.newSource("null_source", 
				"factoryTest", "foo bar baz", "test/source", "gz", 10, 
				pipeline, testLogger);		
		assertEquals(NullSource.class, source.getClass());
		
		source = SourceFactory.newSource("script_source", 
				"factoryTest", "foo bar baz", "test/source", "gz", 10, 
				pipeline, testLogger);		
		assertEquals(ScriptSource.class, source.getClass());
		
		source = SourceFactory.newSource("hbase_source", 
				"factoryTest", "troilkatt-geo-meta meta:organisms334 'Homo sapiens' files:pcl", 
				"test/source", "gz", 10, 
				pipeline, testLogger);		
		assertEquals(HbaseSource.class, source.getClass());
		
		source = SourceFactory.newSource("href_source", 
				"factoryTest", 
				HREFSourceTest.pageURL + " " + HREFSourceTest.filter, 
				"test/source", "gz", 10, 
				pipeline, testLogger);		
		assertEquals(HREFSource.class, source.getClass());
		
		String cmdsFilename = "/tmp/foo";
		OsCmdsSourceTest.createCmdsFile(cmdsFilename);
		source = SourceFactory.newSource("os_cmds_source", 
				"factoryTest", 
				cmdsFilename, 
				"test/source", "gz", 10, 
				pipeline, testLogger);		
		assertEquals(OsCmdsSource.class, source.getClass());
	}

	// Invalid source
	@Test(expected=StageInitException.class)
	public void testNewSource2() throws IOException, TroilkattPropertiesException, StageInitException {				
		Source source = SourceFactory.newSource("unknown_source", 
				"factoryTest", "foo bar baz", "test/source", "gz", 10, 
				pipeline, testLogger);
		assertEquals(ExecuteSource.class, source.getClass());
	}
}
