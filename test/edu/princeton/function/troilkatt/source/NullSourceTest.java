package edu.princeton.function.troilkatt.source;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;

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
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFS;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class NullSourceTest extends TestSuper {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
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
	public void testRetrieve2() throws IOException, TroilkattPropertiesException, StageInitException, StageException, PipelineException {		
		TroilkattProperties troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));		
		FileSystem hdfs = FileSystem.get(new Configuration());			
		TroilkattFS tfs = new TroilkattFS(hdfs);
		Pipeline pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs);
		
		String  localRootDir = tmpDir;
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"), OsPath.join("meta", pipeline.name));
		String hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 0, "nullSource"));
		String hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"), "tmp");
		
		NullSource source = new NullSource("nullSource", "foo bar baz",
				"test/source", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertNotNull(source);
		
		ArrayList<String> rv = source.retrieve2(31);
		assertEquals(0, rv.size());		
	}

	// Nothing to test
	//@Test
	//public void testNullSource() {
	//}

}
