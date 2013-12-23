package edu.princeton.function.troilkatt.pipeline;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

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
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.LogTableHbase;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;
import edu.princeton.function.troilkatt.mapreduce.TroilkattMapReduce;

public class MapReduceTest extends TestSuper {
	protected static TroilkattHDFS tfs;
	protected static LogTableHbase lt;
	protected static Pipeline pipeline;	
	protected static TroilkattProperties troilkattProperties;
	protected static String hdfsOutput;
	protected static Logger testLogger;
	
	public static String testJar = "TROILKATT.JAR";
	public static String testClass = "edu.princeton.function.troilkatt.mapreduce.UnitTest";
	
	protected MapReduce mrs;
	protected ArrayList<String> inputFiles;
	
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
		
		hdfsOutput = "test/mapreduce/output";
		localRootDir = tmpDir;		
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), OsPath.join("meta", pipeline.name));
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 8, "mapreduce-unittest"));
		hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "tmp");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		
	}

	@Before
	public void setUp() throws Exception {
		mrs = new MapReduce(8, "mapreduce-unittest", testJar + " " + testClass + " 256 512 atn1 vcsd1",
				hdfsOutput, "gz", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		String inputDir = "troilkatt/data/test/mapreduce/input";
		inputFiles = new ArrayList<String>();
		inputFiles.add(OsPath.join(inputDir, "file1.1.gz"));
		inputFiles.add(OsPath.join(inputDir, "file2.1.gz"));
		inputFiles.add(OsPath.join(inputDir, "file3.1.gz"));
		inputFiles.add(OsPath.join(inputDir, "file4.1.none"));
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test 
	public void testMapReduce() throws TroilkattPropertiesException, StageInitException {		
		assertTrue(mrs.jarFile.endsWith("troilkatt.jar"));
		assertEquals("atn1 vcsd1", mrs.stageArgs);
		assertEquals("input.args", OsPath.basename(mrs.argsFilename));
		assertNotNull(mrs.mapReduceCmd);
	}

	@Test
	public void testDownloadInputFiles() throws StageException {
		ArrayList<String> downloaded = mrs.downloadInputFiles(inputFiles);
		assertEquals(inputFiles, downloaded);
	}

	@Test
	public void testSaveOutputFiles() throws StageException {
		ArrayList<String> outputFiles = inputFiles;		
		ArrayList<String> downloaded = mrs.saveOutputFiles(outputFiles, 567);
		assertEquals(outputFiles, downloaded);
	}

	@Test
	public void testWriteMapReduceArgsFile() throws StageException, StageInitException, IOException, TroilkattPropertiesException {		
		String hdfsTmpOutputDir = OsPath.join(hdfsStageTmpDir, mrs.pipelineName + "-" + mrs.stageName + "-" + 567);
		mrs.writeMapReduceArgsFile(inputFiles, hdfsTmpOutputDir, 567);
		
		BufferedReader ib = new BufferedReader(new FileReader(mrs.argsFilename));
		
		// Note! read order of lines must match write order in MapReduce.writeMapReduceArgsFile
		assertEquals(troilkattProperties.getConfigFile(), TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "configuration.file"));
		assertEquals(mrs.pipelineName, TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "pipeline.name"));		
		assertEquals("008-mapreduce-unittest", TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "stage.name"));		
		assertEquals("atn1 vcsd1", TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "stage.args"));
		assertEquals(hdfsTmpOutputDir, TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "hdfs.output.dir"));		
		assertEquals("gz", TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "compression.format"));
		assertTrue(Long.valueOf(TroilkattMapReduce.checkKeyGetValLong(ib.readLine(), "storage.time")) == -1);			
		assertEquals(mrs.tfsMetaDir, TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "hdfs.meta.dir"));	
		String mapreduceDir = troilkattProperties.get("troilkatt.localfs.mapreduce.dir");		
		assertEquals(OsPath.join(mapreduceDir, "input"), TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "mapred.input.dir"));
		assertEquals(OsPath.join(mapreduceDir, "output"), TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "mapred.output.dir"));
		assertEquals(OsPath.join(mapreduceDir, "meta"), TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "mapred.meta.dir"));
		assertEquals(OsPath.join(mapreduceDir, "global-meta"), TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "mapred.global-meta.dir"));
		assertEquals(OsPath.join(mapreduceDir, "tmp"), TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "mapred.tmp.dir"));
		assertEquals(mrs.stageInputDir, TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "jobclient.input.dir"));
		assertEquals(mrs.stageOutputDir, TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "jobclient.output.dir"));
		assertEquals(mrs.stageMetaDir, TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "jobclient.meta.dir"));
		assertEquals(mrs.globalMetaDir, TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "jobclient.global-meta.dir"));
		assertEquals(mrs.stageLogDir, TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "jobclient.log.dir"));
		assertEquals(mrs.stageTmpDir, TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "jobclient.tmp.dir"));
		assertNotNull(TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "logging.level"));		
		assertTrue(Long.valueOf(TroilkattMapReduce.checkKeyGetValLong(ib.readLine(), "timestamp")) == 567);
		assertTrue(Long.valueOf(TroilkattMapReduce.checkKeyGetValLong(ib.readLine(), "soft.max.memory.mb")) == 256);
		assertTrue(Long.valueOf(TroilkattMapReduce.checkKeyGetValLong(ib.readLine(), "hard.max.memory.mb")) == 512);		
		assertEquals("input.files.start", ib.readLine());
		
		ArrayList<String> inputFiles2 = new ArrayList<String>();
		while (true) {
			String str = ib.readLine();
			if (str == null) {
				fail("input.files.end was not found");
			}
			if (str.equals("input.files.end")) {
				break;
			}
			inputFiles2.add(str.trim());
		}
		ib.close();
		
		assertEquals(inputFiles.size(), inputFiles2.size());
		for (String f: inputFiles) {
			assertTrue(inputFiles2.contains(f));
		}
	}

	// Non-fail version is not tested since it is implicitly called from testProcess()
	// Invalid jar file
	@Test
	public void testExecuteMapReduceCmd() throws TroilkattPropertiesException, StageInitException, StageException, IOException {
		mrs = new MapReduce(8, "mapreduce-unittest", 
				"NonExisting.jar " + testClass + " 256 512 atn1 vcsd1",
				hdfsOutput, "gz", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		String hdfsTmpOutputDir = OsPath.join(hdfsStageTmpDir, mrs.pipelineName + "-" + mrs.stageName + "-" + 568);
		tfs.deleteDir(hdfsTmpOutputDir);
		assertFalse(Stage.executeCmd(mrs.mapReduceCmd, testLogger) == 0);
	}
	
	//Invalid main class
	@Test
	public void testExecuteMapReduceCmd2() throws TroilkattPropertiesException, StageInitException, StageException, IOException {
		mrs = new MapReduce(8, "mapreduce-unittest", 
				testJar + " foo.bar.Baz 256 512 atn1 vcsd1",
				hdfsOutput, "gz", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		String hdfsTmpOutputDir = OsPath.join(hdfsStageTmpDir, mrs.pipelineName + "-" + mrs.stageName + "-" + 568);
		tfs.deleteDir(hdfsTmpOutputDir);
		assertFalse(Stage.executeCmd(mrs.mapReduceCmd, testLogger) == 0);
	}
	
	// Invalid arguments
	@Test
	public void testExecuteMapReduceCmd3() throws TroilkattPropertiesException, StageInitException, StageException, IOException {
		mrs = new MapReduce(8, "mapreduce-unittest", testJar + " " + testClass + " 256 512",
				hdfsOutput, "gz", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		String hdfsTmpOutputDir = OsPath.join(hdfsStageTmpDir, mrs.pipelineName + "-" + mrs.stageName + "-" + 568);
		tfs.deleteDir(hdfsTmpOutputDir);
		assertFalse(Stage.executeCmd(hdfsTmpOutputDir, testLogger) == 0);
	}

	// Output directory already exist
	@Test
	public void testExecuteMapReduceCmd4() throws TroilkattPropertiesException, StageInitException, StageException, IOException {
		mrs = new MapReduce(8, "mapreduce-unittest", 
				testJar + " " + testClass + " 256 512 atn1 vscd1",
				hdfsOutput, "gz", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		String hdfsTmpOutputDir = OsPath.join(hdfsStageTmpDir, mrs.pipelineName + "-" + mrs.stageName + "-" + 568);
		tfs.mkdir(hdfsTmpOutputDir);
		assertFalse(Stage.executeCmd(mrs.mapReduceCmd, testLogger) == 0);
	}
	
	// Invalid soft MB
	@Test(expected=StageInitException.class)
	public void testExecuteMapReduceCmd5() throws TroilkattPropertiesException, StageInitException, StageException, IOException {
		mrs = new MapReduce(8, "mapreduce-unittest", 
				testJar + " " + testClass + " foo 512 atn1 vcsd1",
				hdfsOutput, "gz", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		String hdfsTmpOutputDir = OsPath.join(hdfsStageTmpDir, mrs.pipelineName + "-" + mrs.stageName + "-" + 568);
		tfs.deleteDir(hdfsTmpOutputDir);
		assertFalse(Stage.executeCmd(mrs.mapReduceCmd, testLogger) == 0);
	}
	
	// Invalid hard MB
	@Test(expected=StageInitException.class)
	public void testExecuteMapReduceCmd6() throws TroilkattPropertiesException, StageInitException, StageException, IOException {
		mrs = new MapReduce(8, "mapreduce-unittest", 
				testJar + " " + testClass + " 256 bar atn1 vcsd1",
				hdfsOutput, "gz", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		String hdfsTmpOutputDir = OsPath.join(hdfsStageTmpDir, mrs.pipelineName + "-" + mrs.stageName + "-" + 568);
		tfs.deleteDir(hdfsTmpOutputDir);
		assertFalse(Stage.executeCmd(mrs.mapReduceCmd, testLogger) == 0);
	}
	
	@Test
	public void testProcess() throws IOException, StageException {		
		ArrayList<String> metaFiles = writeMetaFile(mrs, inputFiles, 731);
		
		ArrayList<String> logFiles = new ArrayList<String>();
		
		System.out.println("Output test: pipeline: System.out.println: before running MapReduce job");
		System.err.println("Output test: pipeline: System.err.println: before running MapReduce job");
		mrs.logger.fatal("Output test: pipeline: logger.fatal: before running MapReduce job");
		
		ArrayList<String> outputFiles = mrs.process(inputFiles, metaFiles, logFiles, 731);
		
		System.out.println("Output test: pipeline: System.out.println: after running MapReduce job");
		System.err.println("Output test: pipeline: System.err.println: after running MapReduce job");
		mrs.logger.fatal("Output test: pipeline: logger.fatal: after running MapReduce job");
		
		assertEquals(2, outputFiles.size());		
		assertTrue(OsPath.fileInList(outputFiles, "atn1", true));
		assertTrue(OsPath.fileInList(outputFiles, "vcsd1", true));
		
		String atn1File = null;
		for (String f: outputFiles) {			
			String basename = OsPath.basename(f);				
			if (basename.contains("atn1")) {
				atn1File = f;
				break;
			}
		}		
		String atn1File2 = tfs.getFile(atn1File, tmpDir, tmpDir, tmpDir);
		String[] lines = FSUtils.readTextFile(atn1File2);
		assertEquals("atn1\t8", lines[0]);
		
		assertEquals(3, metaFiles.size());		
		/* The logfiles are:
		 * 1. mapreduce.error
		 * 2. mapreduce.output
		 */
		assertEquals(2, logFiles.size());	
		assertTrue(OsPath.fileInList(logFiles, "mapreduce.error", false));
		assertTrue(OsPath.fileInList(logFiles, "mapreduce.output", false));
	}
	
	// Invalid files
	@Test
	public void testProcessI() throws IOException, StageException {
		inputFiles.clear();
		String inputDir = "troilkatt/data/test/mapreduce/input";		
		inputFiles.add(OsPath.join(inputDir, "file5.2.bz2"));
		inputFiles.add(OsPath.join(inputDir, "file6.2.7z"));
		
		ArrayList<String> metaFiles = writeMetaFile(mrs, inputFiles, 732);
		
		ArrayList<String> logFiles = new ArrayList<String>();
		
		try {
			mrs.process(inputFiles, metaFiles, logFiles, 732);
		} catch (StageException e) {
			// expected
		}
		
		// Check logfiles
		assertEquals(2, logFiles.size());	
		assertTrue(OsPath.fileInList(logFiles, "mapreduce.error", false));
		assertTrue(OsPath.fileInList(logFiles, "mapreduce.output", false));
	}
	
	// Invalid files 2
	@Test
	public void testProcessI2() throws IOException, StageException {
		inputFiles.clear();
		String inputDir = "troilkatt/data/test/mapreduce/input";		
		inputFiles.add(OsPath.join(inputDir, "file7.3.gz"));
		
		ArrayList<String> metaFiles = writeMetaFile(mrs, inputFiles, 733);
		
		ArrayList<String> logFiles = new ArrayList<String>();
			
		try {
			mrs.process(inputFiles, metaFiles, logFiles, 733);
			fail("StageException should have been thrown");
		} catch (StageException e) {
			// expected
		}
		
		// Check logfiles
		assertEquals(2, logFiles.size());	
		assertTrue(OsPath.fileInList(logFiles, "mapreduce.error", false));
		assertTrue(OsPath.fileInList(logFiles, "mapreduce.output", false));
	}
	
	// No input files
	@Test
	public void testProcessI3() throws IOException, StageException {
		inputFiles.clear();
		
		ArrayList<String> metaFiles = writeMetaFile(mrs, inputFiles, 734);
		
		ArrayList<String> logFiles = new ArrayList<String>();
		
		ArrayList<String> outputFiles = mrs.process(inputFiles, metaFiles, logFiles, 734);
		assertTrue(outputFiles.isEmpty());
		// Check logfiles
		assertEquals(2, logFiles.size());	
		assertTrue(OsPath.fileInList(logFiles, "mapreduce.error", false));
		assertTrue(OsPath.fileInList(logFiles, "mapreduce.output", false));
	}

	// Invalid jar
	@Test
	public void testProcessI4() throws IOException, StageException, TroilkattPropertiesException, StageInitException {
		ArrayList<String> metaFiles = writeMetaFile(mrs, inputFiles, 735);
		
		ArrayList<String> logFiles = new ArrayList<String>();
		
		mrs = new MapReduce(8, "mapreduce-unittest", 
				"NonExisting.jar " + testClass + " 256 512 atn1 vcsd1",
				hdfsOutput, "gz", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		try {
			mrs.process(inputFiles, metaFiles, logFiles, 735);
			fail("StageException should have been thrown");
		} catch (StageException e) {
			// expected
		}
		// Check logfiles
		assertEquals(2, logFiles.size());	
		assertTrue(OsPath.fileInList(logFiles, "mapreduce.error", false));
		assertTrue(OsPath.fileInList(logFiles, "mapreduce.output", false));
	}
	
	@Test
	public void testProcess2() throws IOException, StageException {
		writeMetaFile(mrs, inputFiles, 736);
		
		ArrayList<String> outputFiles = mrs.process2(inputFiles, 736);
		
		/*
		 * Test output files
		 */
		assertEquals(2, outputFiles.size());		
		assertTrue(OsPath.fileInList(outputFiles, "atn1", true));
		assertTrue(OsPath.fileInList(outputFiles, "vcsd1", true));
		for (String f: outputFiles) {
			assertTrue(f.contains(hdfsOutput));
		}
		
		String atn1File = null;
		for (String f: outputFiles) {			
			String basename = OsPath.basename(f);				
			if (basename.contains("atn1")) {
				atn1File = f;
				break;
			}
		}		
		String atn1File2 = tfs.getFile(atn1File, tmpDir, tmpDir, tmpDir);
		String[] lines = FSUtils.readTextFile(atn1File2);
		assertEquals("atn1\t8", lines[0]);
		
		/*
		 * Test MapReduce logfiles
		 */
		OsPath.deleteAll(tmpDir);
		OsPath.mkdir(tmpDir);		
		LogTableHbase lt = (LogTableHbase) mrs.logTable;
		ArrayList<String> logFiles = lt.getMapReduceLogFiles(mrs.stageName, 736, tmpDir);
		assertFalse(logFiles.isEmpty());
		String[] subDirs = OsPath.listdir(tmpDir);
		int mappers = 0;
		int reducers = 0;
		String aMapper = null;
		String aReducer = null;
		for (String s: subDirs) {
			if (OsPath.isdir(s)) {
				String basename = OsPath.basename(s);
				if (basename.contains("attempt") && basename.contains("_m_")) {
					mappers++;
					aMapper = s;
				}
				else if (basename.contains("attempt") && basename.contains("_r_")) {
					reducers++;
					aReducer = s;
				}
			}
		}
		assertTrue(mappers > 0);
		assertTrue(reducers > 0);
		
		String[] mapperFiles = OsPath.listdir(aMapper);
		assertEquals(5, mapperFiles.length);
		assertTrue(OsPath.fileInList(mapperFiles, "stdout", false));
		assertTrue(OsPath.fileInList(mapperFiles, "stderr", false));
		assertTrue(OsPath.fileInList(mapperFiles, "syslog", false));
		assertTrue(OsPath.fileInList(mapperFiles, "foo.txt", false));
		//assertTrue(OsPath.fileInList(mapperFiles, "5-mapreduce-unittest-task.log", false));
		
		String[] reducerFiles = OsPath.listdir(aReducer);
		assertEquals(5, reducerFiles.length);
		assertTrue(OsPath.fileInList(reducerFiles, "stdout", false));
		assertTrue(OsPath.fileInList(reducerFiles, "stderr", false));
		assertTrue(OsPath.fileInList(reducerFiles, "syslog", false));
		assertTrue(OsPath.fileInList(reducerFiles, "foo.txt", false));
		//assertTrue(OsPath.fileInList(reducerFiles, "5-mapreduce-unittest-task.log", false));
	}
	
	// saveOutputFiles is not tested since it is tested as part of testProcess()
	//@Test
	//public void testSaveOutputFiles() {
	//}
	
	public static ArrayList<String> writeMetaFile(MapReduce mrs,
			ArrayList<String> inputFiles, long timestamp) throws IOException, StageException {
		String metaFilename = OsPath.join(mrs.stageMetaDir, "filelist");
		FSUtils.writeTextFile(metaFilename, inputFiles);
		
		String[] succesLine = {"success\n"};
		String mapFilename = OsPath.join(mrs.stageMetaDir, "maptest");
		FSUtils.writeTextFile(mapFilename, succesLine);
		
		String reduceFilename = OsPath.join(mrs.stageMetaDir, "reducetest");
		FSUtils.writeTextFile(reduceFilename, succesLine);
		
		ArrayList<String> metaFiles = new ArrayList<String>();
		metaFiles.add(metaFilename);
		metaFiles.add(mapFilename);
		metaFiles.add(reduceFilename);
		
		mrs.tfs.deleteDir(mrs.tfsMetaDir);
		mrs.tfs.mkdir(mrs.tfsMetaDir);
		mrs.saveMetaFiles(metaFiles, timestamp - 1);
		OsPath.delete(metaFilename);
		return metaFiles;
	}
	
	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main("edu.princeton.function.troilkatt.pipeline.MapReduceTest");
	}
}
