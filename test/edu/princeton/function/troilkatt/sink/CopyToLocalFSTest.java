package edu.princeton.function.troilkatt.sink;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import java.util.Arrays;

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

public class CopyToLocalFSTest extends TestSuper {
	protected CopyToLocalFS sink;	
	protected static String outputDir;
	protected static Logger testLogger;
	protected static Pipeline pipeline;
	
	protected static String localRootDir;
	protected static String hdfsStageMetaDir;
	protected static String hdfsStageTmpDir;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		outputDir = OsPath.join(tmpDir, "sink");
		testLogger = Logger.getLogger("test");
		
		TroilkattProperties troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));
		Configuration hdfsConfig = new Configuration();
		FileSystem hdfs = FileSystem.get(hdfsConfig);			
		TroilkattHDFS tfs = new TroilkattHDFS(hdfs);
		LogTableHbase lt = new LogTableHbase("unitPipeline", HBaseConfiguration.create());
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs, lt);
		
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
		OsPath.deleteAll(outputDir);
		
		sink = new CopyToLocalFS(3, "unitSink", outputDir,	
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testCopyToLocalFS() {
		assertEquals(outputDir, sink.outputDir);
	}

	// Invalid output dir
	@Test(expected=StageInitException.class)
	public void testCopyToLocalFS2() throws IOException, TroilkattPropertiesException, StageInitException, PipelineException {				
		sink = new CopyToLocalFS(3, "unitSink", "/foo/bar/baz",	
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
	}
	
	@Test
	public void testSink() throws StageException {		
		ArrayList<String> inputFiles = new ArrayList<String>();
		ArrayList<String> metaFiles = new ArrayList<String>();
		ArrayList<String> logFiles = new ArrayList<String>();
		
		inputFiles.add("troilkatt/data/test/input/file1.1.none");
		inputFiles.add("troilkatt/data/test/input/file2.1.none");
		inputFiles.add("troilkatt/data/test/input/subdir2/file6.2.none");
		sink.sink(inputFiles, metaFiles, logFiles, 301);
        
        String[] files = OsPath.listdir(sink.outputDir, testLogger);
        
        assertEquals(files.length, 3);
        Arrays.sort(files);
        assertEquals("file1", OsPath.basename(files[0]));
        assertEquals("file2", OsPath.basename(files[1]));
        assertEquals("file6", OsPath.basename(files[2]));
        
        assertEquals(0, metaFiles.size());
        assertEquals(0, logFiles.size());
	}

	// Invalid dir
	@Test(expected=StageException.class)
	public void testSink2() throws StageException {
		sink.outputDir = "/foo/bar/baz";
		
		ArrayList<String> inputFiles = new ArrayList<String>();
		ArrayList<String> metaFiles = new ArrayList<String>();
		ArrayList<String> logFiles = new ArrayList<String>();
		
		inputFiles.add("troilkatt/data/test/input/file1.1.none");
        inputFiles.add("troilkatt/data/test/input/file2.1.none");
        inputFiles.add("troilkatt/data/test/input/subdir2/file6.2.none");

        sink.sink(inputFiles, metaFiles, logFiles, 302);
	}
}
