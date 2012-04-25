package edu.princeton.function.troilkatt.source;

import static org.junit.Assert.*;

import java.util.ArrayList;

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
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFS;
import edu.princeton.function.troilkatt.pipeline.StageException;

public class SourceTest extends TestSuper {
	protected Source source;	
	
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {		
		TroilkattProperties troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));		
		FileSystem hdfs = FileSystem.get(new Configuration());			
		TroilkattFS tfs = new TroilkattFS(hdfs);
		Pipeline pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs);
		
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"), OsPath.join("meta", pipeline.name));
		String hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 0, "unitSource"));
		String hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"), "tmp");	
		source = new Source("unitSource", "foo bar baz",
				"test/source", "gz", 10, tmpDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
	}

	@After
	public void tearDown() throws Exception {
	}

	// Not supported by source
	@Test(expected=RuntimeException.class)
	public void testProcess2() throws StageException {
		source.process2(new ArrayList<String>(), 99);
	}

	// Not unit tested
	//@Test(expected=RuntimeException.class)
	//public void testRecover() throws StageException {
	//	source.recover(98);
	//}

	// No input files to download for source
	@Test(expected=RuntimeException.class)
	public void testDownloadInputFiles() throws StageException {
		source.downloadInputFiles(new ArrayList<String>());
	}

	// Not supported
	@Test(expected=RuntimeException.class)
	public void testProcess() throws StageException {
		source.process(new ArrayList<String>(), new ArrayList<String>(), new ArrayList<String>(), 99);
	}

	@Test
	public void testIsSource() {
		assertTrue(source.isSource());
	}

	// Constructor not tested since it just calls the superclass
	//@Test
	//public void testSource() {
	//}

	// Not unit tested
	//@Test
	//public void testRetrieve2() {
	//}

	@Test(expected=RuntimeException.class)
	public void testRetrieve() throws StageException {
		source.retrieve(new ArrayList<String>(), new ArrayList<String>(), 99);
	}

}
