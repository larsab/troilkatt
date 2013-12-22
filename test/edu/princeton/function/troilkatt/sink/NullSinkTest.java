package edu.princeton.function.troilkatt.sink;

import static org.junit.Assert.*;

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
import edu.princeton.function.troilkatt.TestSuper;
import edu.princeton.function.troilkatt.Troilkatt;
import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.fs.LogTableHbase;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;
import edu.princeton.function.troilkatt.pipeline.StageException;

public class NullSinkTest extends TestSuper {
	protected NullSink sink;
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
		LogTableHbase lt = new LogTableHbase("unitPipeline", HBaseConfiguration.create());
		Pipeline pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs, lt);
		
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
		sink = new NullSink(3, "unitSink", "",	
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testNullSink() {
		assertNotNull(sink);
	}

	@Test
	public void testRecover() throws StageException {
		sink.recover(new ArrayList<String>(), 310);
	}

	@Test
	public void testSink() throws StageException {
		ArrayList<String> inputFiles = new ArrayList<String>();
		ArrayList<String> metaFiles = new ArrayList<String>();
		ArrayList<String> logFiles = new ArrayList<String>();		
		sink.sink(inputFiles, metaFiles, logFiles, 311);
	}

}
