package edu.princeton.function.troilkatt.sink;

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
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;
import edu.princeton.function.troilkatt.pipeline.StageException;

public class SinkTest extends TestSuper {
	protected Sink sink;	
	protected static Pipeline pipeline;
	
	protected static String localRootDir;
	protected static String hdfsStageMetaDir;
	protected static String hdfsStageTmpDir;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TroilkattProperties troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));
		Configuration hdfsConfig = new Configuration();
		FileSystem hdfs = FileSystem.get(hdfsConfig);			
		TroilkattHDFS tfs = new TroilkattHDFS(hdfs);
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs);
		
		localRootDir = tmpDir;		
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), OsPath.join("meta", pipeline.name));
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 3, "unitSink"));
		hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "tmp");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {		
		sink = new Sink(3, "unitSink", "foo bar baz",
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
	}

	@After
	public void tearDown() throws Exception {
	}

	// Not supported by sink
	@Test(expected=RuntimeException.class)
	public void testProcess2() throws StageException {
		sink.process2(new ArrayList<String>(), 99);
	}
	
	// Must be implemented by subclass
	@Test(expected=RuntimeException.class)
	public void testRecover() throws StageException {
		sink.recover(new ArrayList<String>(), 98);
	}

	// No input files to download for sink
	@Test
	public void testDownloadInputFiles() throws StageException {
		ArrayList<String> downloaded = sink.downloadInputFiles(new ArrayList<String>());
		assertTrue(downloaded.isEmpty());		
	}

	// Not supported
	@Test(expected=RuntimeException.class)
	public void testProcess() throws StageException {
		sink.process(new ArrayList<String>(), new ArrayList<String>(), new ArrayList<String>(), 99);
	}

	// Must be implemented by subclass
	@Test(expected=RuntimeException.class)
	public void testSink() throws StageException {
		sink.sink(new ArrayList<String>(), new ArrayList<String>(), 
				new ArrayList<String>(), 97);
	}

	// Not unit tested
	//@Test(expected=RuntimeException.class)
	//public void testSink2() {
	//}


	// Sink does not save output files
	@Test(expected=RuntimeException.class)
	public void testSaveOutputFiles() throws StageException {
		sink.saveOutputFiles(new ArrayList<String>());
	}

}
