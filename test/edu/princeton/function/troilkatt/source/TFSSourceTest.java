package edu.princeton.function.troilkatt.source;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
import edu.princeton.function.troilkatt.fs.LogTableHbase;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class TFSSourceTest extends TestSuper {

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

	// Not unit tested
	//@Test
	//public void testRetrieve2() {
	//}
	
	@Test
	public void testHDFSSource() throws IOException, TroilkattPropertiesException, StageInitException, PipelineException {
		TroilkattProperties troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));		
		FileSystem hdfs = FileSystem.get(new Configuration());			
		TroilkattHDFS tfs = new TroilkattHDFS(hdfs);
		LogTableHbase lt = new LogTableHbase("unitPipeline", HBaseConfiguration.create());
		Pipeline pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs, lt);
		
		String localRootDir = tmpDir;
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), OsPath.join("meta", pipeline.name));
		String hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 0, "HDFSSourceUnit"));
		String hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "tmp");
		
		TFSSource source = new TFSSource("unitSource", "foo bar baz",
				"test/source", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertNotNull(source);
	}

	@Test(expected=RuntimeException.class)
	public void testSaveOutputFiles() throws TroilkattPropertiesException, StageInitException, IOException, StageException, PipelineException {
		TroilkattProperties troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));		
		FileSystem hdfs = FileSystem.get(new Configuration());			
		TroilkattHDFS tfs = new TroilkattHDFS(hdfs);
		LogTableHbase lt = new LogTableHbase("unitPipeline", HBaseConfiguration.create());
		Pipeline pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs, lt);
		
		String localRootDir = tmpDir;
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), OsPath.join("meta", pipeline.name));
		String hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 0, "HDFSSourceUnit"));
		String hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "tmp");
		
		TFSSource source = new TFSSource("HDFSSourceUnit", "foo bar baz",				
				"test/source", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		source.saveOutputFiles(new ArrayList<String>(), 10);		
	}

}
