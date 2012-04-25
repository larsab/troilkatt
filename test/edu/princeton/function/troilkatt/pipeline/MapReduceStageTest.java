package edu.princeton.function.troilkatt.pipeline;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
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
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFS;

public class MapReduceStageTest extends TestSuper {
	protected static TroilkattFS tfs;
	protected static Pipeline pipeline;	
	protected static TroilkattProperties troilkattProperties;
	protected static String hdfsOutput;
	protected static Logger testLogger;
	public static String executeCmd = "/usr/bin/python " + OsPath.join(dataDir, "bin/executePerFileTest.py") + " TROILKATT.INPUT_DIR/TROILKATT.FILE TROILKATT.OUTPUT_DIR/TROILKATT.FILE_NOEXT.out TROILKATT.META_DIR/filelist > TROILKATT.LOG_DIR/executePerFileTest.out 2> TROILKATT.LOG_DIR/executePerFileTest.log";
	
	protected MapReduceStage mrs;
	protected ArrayList<String> inputFiles;
	protected ArrayList<String> inputBasenames;
	
	protected static int stageNum;
	protected static String stageName;
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
		tfs = new TroilkattFS(hdfs);
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs);
		
		hdfsOutput = "test/mapreducestage/output";
		
		stageNum = 6;
		stageName = "mapreducestage";
		localRootDir = tmpDir;		
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"), OsPath.join("meta", pipeline.name));
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", stageNum, stageName));
		hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"), "tmp");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		mrs = new MapReduceStage(stageNum, stageName, "execute_per_file " + executeCmd,
				hdfsOutput, "gz", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		String inputDir = "troilkatt/data/test/mapreduce/input";
		inputFiles = new ArrayList<String>();
		inputFiles.add(OsPath.join(inputDir, "file1.1.gz"));
		inputFiles.add(OsPath.join(inputDir, "file2.1.gz"));
		inputFiles.add(OsPath.join(inputDir, "file3.1.gz"));
		inputFiles.add(OsPath.join(inputDir, "file4.1.none"));
		
		inputBasenames = new ArrayList<String>();
		for (String f: inputFiles) {
			String b = OsPath.basename(f);
			inputBasenames.add(b.split("\\.")[0]);
		}
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testMapReduceStage() {
		assertTrue(mrs.jarFile.endsWith("troilkatt.jar"));
		assertTrue(mrs.stageArgs.contains("execute_per_file " + "/usr/bin/python " + OsPath.join(dataDir, "bin/executePerFileTest.py")));
		assertEquals("input.args", OsPath.basename(mrs.argsFilename));
		assertNotNull(mrs.mapReduceCmd);
	}

	@Test
	public void testProcess2() throws IOException, StageException {
		MapReduceTest.writeMetaFile(mrs, inputBasenames, 741);
		
		ArrayList<String> outputFiles = mrs.process2(inputFiles, 741);
		
		/*
		 * Test output files
		 */
		assertEquals(4, outputFiles.size());
		Collections.sort(outputFiles);
		assertTrue(outputFiles.get(0).endsWith("file1.out.741.gz"));
		assertTrue(outputFiles.get(1).endsWith("file2.out.741.gz"));
		assertTrue(outputFiles.get(3).endsWith("file4.out.741.gz"));
		assertTrue(tfs.isfile(OsPath.join(mrs.hdfsOutputDir, "file1.out.741.gz")));
		assertTrue(tfs.isfile(OsPath.join(mrs.hdfsOutputDir, "file2.out.741.gz")));
		assertTrue(tfs.isfile(OsPath.join(mrs.hdfsOutputDir, "file4.out.741.gz")));
		
		/*
		 * Test MapReduce logfiles
		 */
		OsPath.deleteAll(tmpDir);
		OsPath.mkdir(tmpDir);		
		ArrayList<String> logFiles = mrs.logTable.getMapReduceLogFiles(mrs.stageName, 741, tmpDir);
		assertFalse(logFiles.isEmpty());
		String[] subDirs = OsPath.listdir(tmpDir);
		int mappers = 0;
		int reducers = 0;
		String aMapper = null;
		for (String s: subDirs) {
			if (OsPath.isdir(s)) {
				String basename = OsPath.basename(s);
				if (basename.contains("attempt") && basename.contains("_m_")) {
					mappers++;
					aMapper = s;
				}
				else if (basename.contains("attempt") && basename.contains("_r_")) {
					reducers++;
				}
			}
		}
		assertTrue(mappers > 0);
		assertTrue(reducers == 0);
		
		String[] mapperFiles = OsPath.listdir(aMapper);
		assertEquals(8, mapperFiles.length);
		assertTrue(OsPath.fileInList(mapperFiles, "stdout", false));
		assertTrue(OsPath.fileInList(mapperFiles, "stderr", false));
		assertTrue(OsPath.fileInList(mapperFiles, "syslog", false));
		assertTrue(OsPath.fileInList(mapperFiles, "executePerFileTest.log", false));
		assertTrue(OsPath.fileInList(mapperFiles, "executePerFileTest.out", false));
	}
	
	// Invalid stage: should be handled by constructor
	@Test(expected=StageInitException.class)
	public void testProcess2I1() throws IOException, StageException, TroilkattPropertiesException, StageInitException {
		mrs = new MapReduceStage(stageNum, stageName, "invalid_stage " + executeCmd,
				hdfsOutput, "gz", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
	}
	
	// Invalid stage arguments: MapReduce job should return zero output files
	@Test
	public void testProcess2I2() throws IOException, StageException, TroilkattPropertiesException, StageInitException {
		mrs = new MapReduceStage(stageNum, stageName, "execute_per_file foo bar",
				hdfsOutput, "gz", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		MapReduceTest.writeMetaFile(mrs, inputBasenames, 742);
				
		ArrayList<String> outputFiles = mrs.process2(inputFiles, 742);
		assertEquals(0, outputFiles.size());
		
		// Log files should still have been created 
		OsPath.deleteAll(tmpDir);
		OsPath.mkdir(tmpDir);		
		ArrayList<String> logFiles = mrs.logTable.getMapReduceLogFiles(mrs.stageName, 741, tmpDir);
		assertFalse(logFiles.isEmpty());
		assertTrue(OsPath.fileInList(logFiles, "mapreduce.output", false));
		assertTrue(OsPath.fileInList(logFiles, "mapreduce.error", false));
		
		String[] subDirs = OsPath.listdir(tmpDir);
		int mappers = 0;
		int reducers = 0;
		String aMapper = null;
		for (String s: subDirs) {
			if (OsPath.isdir(s)) {
				String basename = OsPath.basename(s);
				if (basename.contains("attempt") && basename.contains("_m_")) {
					mappers++;
					aMapper = s;
				}
				else if (basename.contains("attempt") && basename.contains("_r_")) {
					reducers++;
				}
			}
		}
		assertTrue(mappers > 0);
		assertTrue(reducers == 0);
		
		String[] mapperFiles = OsPath.listdir(aMapper);
		assertEquals(8, mapperFiles.length);
		assertTrue(OsPath.fileInList(mapperFiles, "stdout", false));
		assertTrue(OsPath.fileInList(mapperFiles, "stderr", false));
		assertTrue(OsPath.fileInList(mapperFiles, "syslog", false));
		assertTrue(OsPath.fileInList(mapperFiles, "executePerFileTest.log", false));
		assertTrue(OsPath.fileInList(mapperFiles, "executePerFileTest.out", false));
	}
	
	// Invalid input file: Exception should be handled by MapReduce job	
	@Test
	public void testProcess2I3() throws IOException, StageException, TroilkattPropertiesException, StageInitException {
		mrs = new MapReduceStage(stageNum, stageName, "execute_per_file " + executeCmd,
				hdfsOutput, "gz", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);

		inputFiles.add("troilkatt/data/test/mapreduce/input/invalid.5.gz");
		inputBasenames.add("invalid");

		MapReduceTest.writeMetaFile(mrs, inputBasenames, 743);

		ArrayList<String> outputFiles = mrs.process2(inputFiles, 743);
		assertEquals(0, outputFiles.size());
	}
	
	// Non-existing input file: crashes job
	@Test
	public void testProcess2I4() throws IOException, StageException, TroilkattPropertiesException, StageInitException {
		mrs = new MapReduceStage(stageNum, stageName, "execute_per_file " + executeCmd,
				hdfsOutput, "gz", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);

		inputFiles.add("troilkatt/data/test/mapreduce/input/does-notexist.6.gz");
		inputBasenames.add("does-notexist");

		MapReduceTest.writeMetaFile(mrs, inputBasenames, 744);

		try {
			mrs.process2(inputFiles, 744);
			fail("Should have thrown StageException");
		} catch (StageException e) {
			// expected
		}
		
		// Log files should still have been created 
		OsPath.deleteAll(tmpDir);
		OsPath.mkdir(tmpDir);		
		ArrayList<String> logFiles = mrs.logTable.getMapReduceLogFiles(mrs.stageName, 744, tmpDir);
		assertFalse(logFiles.isEmpty());
		assertTrue(OsPath.fileInList(logFiles, "mapreduce.output", false));
		assertTrue(OsPath.fileInList(logFiles, "mapreduce.error", false));		
		
		String[] subDirs = OsPath.listdir(tmpDir);
		int mappers = 0;
		int reducers = 0;
		for (String s: subDirs) {
			if (OsPath.isdir(s)) {
				String basename = OsPath.basename(s);
				if (basename.contains("attempt") && basename.contains("_m_")) {
					mappers++;
				}
				else if (basename.contains("attempt") && basename.contains("_r_")) {
					reducers++;
				}
			}
		}
		assertTrue(mappers == 0);
		assertTrue(reducers == 0);		
	}
	
	
}
