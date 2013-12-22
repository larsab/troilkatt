package edu.princeton.function.troilkatt.mapreduce;

import static org.junit.Assert.*;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.log4j.Logger;
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
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.LogTableHbase;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;
import edu.princeton.function.troilkatt.pipeline.MapReduce;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class TroilkattMapReduceTest extends TestSuper {
	protected static TroilkattHDFS tfs;
	protected static Pipeline pipeline;	
	protected static TroilkattProperties troilkattProperties;
	protected static String hdfsOutput;
	protected static Logger testLogger;
	protected static LogTableHbase lt;
	
	protected static String testJar = "$TROILKATT_JAR";
	protected static String testClass = "edu.princeton.function.troilkatt.mapreduce.UnitTest";
	
	protected MapReduce mrs;
	
	protected final static String[] randomLines = {"foo\n", "bar\n", "baz\n"}; 
		
	protected static String stageName = "005-mapreduce-unittest";
	protected static String localRootDir;
	protected static String hdfsStageMetaDir;
	protected static String hdfsStageTmpDir;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		testLogger = Logger.getLogger("test");
		TestSuper.initTestDir();
		
		troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));
		
		Configuration hdfsConfig = new Configuration();
		FileSystem hdfs = FileSystem.get(hdfsConfig);			
		tfs = new TroilkattHDFS(hdfs);
		lt = new LogTableHbase("unitPipeline", HBaseConfiguration.create());
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs, lt);
		
		localRootDir = tmpDir;
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"),
				OsPath.join("meta", pipeline.name));
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, stageName);
		hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "tmp");		
		
		hdfsOutput = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"),
				"test/mapreduce/output");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		mrs = new MapReduce(5, "mapreduce-unittest", testJar + " " + testClass + " 256 512 add1 vcsd1",
				hdfsOutput, "gz", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);	
		
		String inputDir = "troilkatt/data/test/mapreduce/input";
		ArrayList<String> inputFiles = new ArrayList<String>();
		inputFiles.add(OsPath.join(inputDir, "file1.1.gz"));
		inputFiles.add(OsPath.join(inputDir, "file2.1.gz"));
		inputFiles.add(OsPath.join(inputDir, "file3.1.gz"));
		inputFiles.add(OsPath.join(inputDir, "file4.1.none"));
		
		String metaFilename = OsPath.join(mrs.stageMetaDir, "filelist");
		FSUtils.writeTextFile(metaFilename, inputFiles);
		ArrayList<String> metaFiles = new ArrayList<String>();
		metaFiles.add(metaFilename);
		mrs.saveMetaFiles(metaFiles, 740);
		OsPath.delete(metaFilename);
		
		String hdfsTmpOutputDir = OsPath.join(mrs.tfsTmpDir, "unitPipeline-005-mapreduce-unittest-741");
		mrs.writeMapReduceArgsFile(inputFiles, hdfsTmpOutputDir, 741);
	}

	@After
	public void tearDown() throws Exception {
	}

	// Tested as part of pipeline.MapReduceTest which starts the MapReduce UnitTest
	// job that includes a call to this function
	//@Test
	//public void testSetInputPaths() {
	//}

	// Tested as part of pipeline.MapReduceTest which starts the MapReduce UnitTest
	// job that includes a call to this function
	//@Test
	//public void testSetOutputPath() {
	//}

	@Test
	public void testParseArgs() throws IOException {
		TroilkattMapReduce tmr = new TroilkattMapReduce();
		Configuration conf = new Configuration();
		
		String[] args = {mrs.argsFilename};
		assertTrue(tmr.parseArgs(conf, args));
		
		assertNotNull(TroilkattMapReduce.jobLogger);
		assertEquals(4, tmr.inputFiles.size());
		assertNotNull(tmr.hdfsOutputDir);
		assertEquals("005-mapreduce-unittest-mr", tmr.progName);
		
		// Too few arguments
		String[] invalidArgs1 = new String[0];
		assertFalse(tmr.parseArgs(conf, invalidArgs1));
		
		// Inavlid filename
		String[] invalidArgs2 = {"/foo/bar/baz"};
		assertFalse(tmr.parseArgs(conf, invalidArgs2));
		
		// Invalid arguments file (missing line)				
		String[] lines = FSUtils.readTextFile(mrs.argsFilename);
		PrintWriter out = new PrintWriter(new FileWriter(mrs.argsFilename + ".invalid"));
		for (int i = 0; i < lines.length; i++) {
			if (i == 3) {
				continue; // skip line
			}
			out.write(lines[i]);
		}
		out.close();
		args[0] = mrs.argsFilename + ".invalid";
		assertFalse(tmr.parseArgs(conf, args));
	}

	@Test
	public void testReadMapReduceArgsFile() {
		TroilkattMapReduce tmr = new TroilkattMapReduce();
		Configuration conf = new Configuration();
		
		String[] args = {mrs.argsFilename};
		assertTrue(tmr.parseArgs(conf, args));
		
		assertNotNull(conf.get("troilkatt.configuration.file"));
		assertNotNull(conf.get("troilkatt.pipeline.name"));		
		assertNotNull(conf.get("troilkatt.stage.name"));
		assertNotNull(conf.get("troilkatt.stage.args"));
		assertNotNull(conf.get("troilkatt.hdfs.output.dir"));
		assertNotNull(conf.get("troilkatt.compression.format"));
		assertNotNull(conf.get("troilkatt.storage.time"));
		assertNotNull(conf.get("troilkatt.hdfs.meta.dir"));
		assertNotNull(conf.get("troilkatt.localfs.input.dir"));
		assertNotNull(conf.get("troilkatt.localfs.output.dir"));
		assertNotNull(conf.get("troilkatt.localfs.meta.dir"));
		assertNotNull(conf.get("troilkatt.globalfs.global-meta.dir"));
		assertNotNull(conf.get("troilkatt.localfs.tmp.dir"));
		assertNotNull(conf.get("troilkatt.jobclient.input.dir"));
		assertNotNull(conf.get("troilkatt.jobclient.output.dir"));
		assertNotNull(conf.get("troilkatt.jobclient.meta.dir"));
		assertNotNull(conf.get("troilkatt.jobclient.global-meta.dir"));
		assertNotNull(conf.get("troilkatt.jobclient.log.dir"));
		assertNotNull(conf.get("troilkatt.jobclient.tmp.dir"));
		assertNotNull(conf.get("troilkatt.logging.level"));		
		assertNotNull(conf.get("troilkatt.timestamp"));
		
		assertNotNull(TroilkattMapReduce.jobLogger);
		assertEquals(4, tmr.inputFiles.size());
		assertTrue(OsPath.fileInList(tmr.inputFiles, "file1.1.gz", false));
		assertTrue(OsPath.fileInList(tmr.inputFiles, "file2.1.gz", false));
		assertTrue(OsPath.fileInList(tmr.inputFiles, "file3.1.gz", false));
		assertTrue(OsPath.fileInList(tmr.inputFiles, "file4.1.none", false));
	}

	@Test
	public void testCheckKeyGetVal() throws StageInitException {
		assertEquals("bar", TroilkattMapReduce.checkKeyGetVal("foo=bar", "foo"));
		assertEquals("bar", TroilkattMapReduce.checkKeyGetVal("foo = bar", "foo"));		
	}
	
	@Test(expected=StageInitException.class)
	public void testCheckKeyGetVal2() throws StageInitException {
		assertEquals("bar", TroilkattMapReduce.checkKeyGetVal("foo=bar", "bar"));				
	}

	@Test
	public void testCheckKeyGetValInt() throws StageInitException {
		assertEquals("5", TroilkattMapReduce.checkKeyGetValInt("foo=5", "foo"));
		assertEquals("6", TroilkattMapReduce.checkKeyGetValInt("foo = 6", "foo"));	
	}
	
	@Test(expected=StageInitException.class)
	public void testCheckKeyGetValInt2() throws StageInitException {
		assertEquals("5", TroilkattMapReduce.checkKeyGetValInt("foo=5", "bar"));
	}

	@Test
	public void testCheckKeyGetValLong() throws StageInitException {
		assertEquals("5", TroilkattMapReduce.checkKeyGetValLong("foo=5", "foo"));
		assertEquals("5", TroilkattMapReduce.checkKeyGetValLong("foo = 5", "foo"));
	}
	
	@Test(expected=StageInitException.class)
	public void testCheckKeyGetValLong2() throws StageInitException {
		assertEquals("5", TroilkattMapReduce.checkKeyGetValLong("foo=5", "bar"));
	}

	@Test
	public void testConfEget() throws IOException {
		Configuration conf = new Configuration();
		conf.set("foo", "bar");
		conf.set("baz", "5");
		assertEquals("bar", TroilkattMapReduce.confEget(conf, "foo"));
		assertEquals("5", TroilkattMapReduce.confEget(conf, "baz"));
	}
	
	@Test(expected=IOException.class)
	public void testConfEget2() throws IOException {
		Configuration conf = new Configuration();
		conf.set("foo", "bar");
		conf.set("baz", "5");
		assertEquals("bar", TroilkattMapReduce.confEget(conf, "bongo"));		
	}

	@Test
	public void testDownloadMetaFiles() throws IOException, StageException {
		ArrayList<String> inputFiles = new ArrayList<String>();
		String inputDir = "troilkatt/data/test/mapreduce/input";
		inputFiles = new ArrayList<String>();
		inputFiles.add(OsPath.join(inputDir, "file1.1.gz"));
		inputFiles.add(OsPath.join(inputDir, "file2.1.gz"));
		inputFiles.add(OsPath.join(inputDir, "file3.1.gz"));
		inputFiles.add(OsPath.join(inputDir, "file4.1.none"));
		
		String metaFilename = OsPath.join(mrs.stageMetaDir, "filelist");
		FSUtils.writeTextFile(metaFilename, inputFiles);
		ArrayList<String> metaFiles = new ArrayList<String>();
		metaFiles.add(metaFilename);
		mrs.saveMetaFiles(metaFiles, 744);
		OsPath.delete(metaFilename);
		
		
		TroilkattMapReduce tmr = new TroilkattMapReduce();
		Configuration conf = new Configuration();		
		String[] args = {mrs.argsFilename};
		assertTrue(tmr.parseArgs(conf, args));
		String hdfsMetaDir = TroilkattMapReduce.confEget(conf, "troilkatt.hdfs.meta.dir");
		String stageMetaDir = TroilkattMapReduce.confEget(conf, "troilkatt.jobclient.meta.dir");
		String stageTmpDir = TroilkattMapReduce.confEget(conf, "troilkatt.jobclient.tmp.dir");
		String stageLogDir = TroilkattMapReduce.confEget(conf, "troilkatt.jobclient.log.dir");
		
		OsPath.deleteAll(stageMetaDir);
		OsPath.mkdir(stageMetaDir);
		
		TroilkattMapReduce.downloadMetaFiles(tfs, hdfsMetaDir, stageMetaDir, stageTmpDir, stageLogDir);
		
		assertTrue(OsPath.isfile(OsPath.join(stageMetaDir, "filelist")));
	}

	// setupTaksLogger(), getTaksLogDir() and saveTaskAttemptLogFiles() are tested as 
	// part of the UnitTest MapReduce job
	
	@Test
	public void testGetTaskLocalMetaDir() throws IOException {
		Configuration conf = new Configuration();
		conf.set("troilkatt.localfs.meta.dir", tmpDir);
				
		String taskMetaDir = TroilkattMapReduce.getTaskLocalMetaDir(conf, "jobid", "taid");
		assertTrue(OsPath.isdir(taskMetaDir));
		FSUtils.writeTextFile(OsPath.join(taskMetaDir, "foo.txt"), randomLines);
		OsPath.deleteAll(taskMetaDir);
		assertFalse(OsPath.isdir(taskMetaDir));
	}
	
	// Invalid configuration file
	@Test(expected=IOException.class)
	public void testGetTaskLocalMetaDir2() throws IOException {
		Configuration conf = new Configuration();		
				
		String taskMetaDir = TroilkattMapReduce.getTaskLocalMetaDir(conf, "jobid", "taid");
		assertTrue(OsPath.isdir(taskMetaDir));	
	}
	
	// Invalid root dir
	@Test(expected=IOException.class)
	public void testGetTaskLocalMetaDir3() throws IOException {
		Configuration conf = new Configuration();		
		conf.set("troilkatt.localfs.meta.dir", "/invalid/dir");		
		String taskMetaDir = TroilkattMapReduce.getTaskLocalMetaDir(conf, "jobid", "taid");
		assertTrue(OsPath.isdir(taskMetaDir));	
	}
	
	@Test
	public void testGetTaskLocalTmpDir() throws IOException {
		Configuration conf = new Configuration();
		conf.set("troilkatt.localfs.tmp.dir", tmpDir);
				
		String taskTmpDir = TroilkattMapReduce.getTaskLocalTmpDir(conf, "jobid", "taid");
		assertTrue(OsPath.isdir(taskTmpDir));
		FSUtils.writeTextFile(OsPath.join(taskTmpDir, "foo.txt"), randomLines);
		OsPath.deleteAll(taskTmpDir);
		assertFalse(OsPath.isdir(taskTmpDir));
	}
	
	// Invalid configuration file
	@Test(expected=IOException.class)
	public void testGetTaskLocalTmpDir2() throws IOException {
		Configuration conf = new Configuration();		
				
		String taskTmpDir = TroilkattMapReduce.getTaskLocalTmpDir(conf, "jobid", "taid");
		assertTrue(OsPath.isdir(taskTmpDir));	
	}
	
	// Invalid root dir
	@Test(expected=IOException.class)
	public void testGetTaskLocalTmpDir3() throws IOException {
		Configuration conf = new Configuration();		
		conf.set("troilkatt.localfs.tmp.dir", "/invalid/dir");		
		String taskTmpDir = TroilkattMapReduce.getTaskLocalTmpDir(conf, "jobid", "taid");
		assertTrue(OsPath.isdir(taskTmpDir));	
	}
	
	@Test
	public void testGetTaskLocalInputDir() throws IOException {
		Configuration conf = new Configuration();
		conf.set("troilkatt.localfs.input.dir", tmpDir);
				
		String taskInputDir = TroilkattMapReduce.getTaskLocalInputDir(conf, "jobid", "taid");
		assertTrue(OsPath.isdir(taskInputDir));
		FSUtils.writeTextFile(OsPath.join(taskInputDir, "foo.txt"), randomLines);
		OsPath.deleteAll(taskInputDir);
		assertFalse(OsPath.isdir(taskInputDir));
	}
	
	// Invalid configuration file
	@Test(expected=IOException.class)
	public void testGetTaskLocalInputDir2() throws IOException {
		Configuration conf = new Configuration();		
				
		String taskInputDir = TroilkattMapReduce.getTaskLocalInputDir(conf, "jobid", "taid");
		assertTrue(OsPath.isdir(taskInputDir));	
	}
	
	// Invalid root dir
	@Test(expected=IOException.class)
	public void testGetTaskLocalInputDir3() throws IOException {
		Configuration conf = new Configuration();		
		conf.set("troilkatt.localfs.input.dir", "/invalid/dir");		
		String taskInputDir = TroilkattMapReduce.getTaskLocalInputDir(conf, "jobid", "taid");
		assertTrue(OsPath.isdir(taskInputDir));	
	}
	
	@Test 
	public void testGetTasktfsOutputDir() throws IOException {
		Configuration conf = new Configuration();		
		conf.set("mapred.work.output.dir", hdfsOutput);
		String hdfsTaskDir = TroilkattMapReduce.getTaskHDFSOutputDir(conf);
		assertTrue(tfs.isdir(hdfsTaskDir));
	}
	
	// Invalid configuration file
	@Test(expected=IOException.class)
	public void testGetTasktfsOutputDir2() throws IOException {
		Configuration conf = new Configuration();				
		String hdfsTaskDir = TroilkattMapReduce.getTaskHDFSOutputDir(conf);
		assertTrue(tfs.isdir(hdfsTaskDir));
	}
	
	// invalid root dir
	//@Test(expected=IOException.class)
	//public void testGetTasktfsOutputDir3() throws IOException {
	//	Configuration conf = new Configuration();		
	//	conf.set("mapred.work.output.dir", "/invalid/dir");
	//	String hdfsTaskDir = TroilkattMapReduce.getTasktfsOutputDir(conf);
	//	assertTrue(tfs.isdir(hdfsTaskDir));
	//}
	
	@Test
	public void testGetTaskLocalOutputDir() throws IOException {
		Configuration conf = new Configuration();
		conf.set("troilkatt.localfs.output.dir", tmpDir);
		conf.set("troilkatt.localfs.tmp.dir", tmpDir);
		conf.set("mapred.work.output.dir", hdfsOutput);
		
		String taskTmpDir = TroilkattMapReduce.getTaskLocalTmpDir(conf, "jobid", "taid");
		String taskOutputDir = TroilkattMapReduce.getTaskLocalOutputDir(conf, "jobid", "taid");
		
		assertTrue(OsPath.isdir(taskOutputDir));
		FSUtils.writeTextFile(OsPath.join(taskOutputDir, "foo.txt"), randomLines);	
		FSUtils.writeTextFile(OsPath.join(taskOutputDir, "bar.txt"), randomLines);		
		FSUtils.writeTextFile(OsPath.join(taskOutputDir, "baz.txt"), randomLines);		
		
		ArrayList<String> hdfsFilenames = TroilkattMapReduce.saveTaskOutputFiles(tfs, conf, taskOutputDir, taskTmpDir, tmpDir, "gz", 760);
		assertEquals(3, hdfsFilenames.size());
		Collections.sort(hdfsFilenames);
		for (String h: hdfsFilenames) {			
			assertTrue(tfs.isfile(h));
		}
		assertEquals("bar.txt.gz", OsPath.basename(hdfsFilenames.get(0)));
		assertEquals("baz.txt.gz", OsPath.basename(hdfsFilenames.get(1)));
		assertEquals("foo.txt.gz", OsPath.basename(hdfsFilenames.get(2)));
		
		assertTrue(OsPath.isdir(taskOutputDir));
	}
	
	// Invalid configuration file
	@Test(expected=IOException.class)
	public void testGetTaskLocalOutputDir2() throws IOException {
		Configuration conf = new Configuration();		
				
		String taskOutputDir = TroilkattMapReduce.getTaskLocalOutputDir(conf, "jobid", "taid");
		assertTrue(OsPath.isdir(taskOutputDir));	
	}
	
	// Invalid root dir
	@Test(expected=IOException.class)
	public void testGetTaskLocalOutputDir3() throws IOException {
		Configuration conf = new Configuration();		
		conf.set("troilkatt.localfs.output.dir", "/invalid/dir");		
		String taskOutputDir = TroilkattMapReduce.getTaskLocalOutputDir(conf, "jobid", "taid");
		assertTrue(OsPath.isdir(taskOutputDir));	
	}
	

}
