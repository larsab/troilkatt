package edu.princeton.function.troilkatt.mapreduce;


import static org.junit.Assert.assertEquals;

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
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;
import edu.princeton.function.troilkatt.pipeline.MapReduce;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class SplitSoftTest extends TestSuper {
	protected static TroilkattHDFS tfs;
	protected static Pipeline pipeline;	
	protected static TroilkattProperties troilkattProperties;
	protected static String hdfsOutput;
	protected static Logger testLogger;
	
	protected static String testJar = "TROILKATT.JAR";
	protected static String testClass = "edu.princeton.function.troilkatt.mapreduce.SplitSoft";

	protected static String inputHDFSDir = "troilkatt/data/test/mapreduce/input";
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
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs);
		
		localRootDir = tmpDir;
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"),
				OsPath.join("meta", pipeline.name));
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 5, "mapreduce-splittest"));
		hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "tmp");		
		
		
		hdfsOutput = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"),
				"test/mapreduce/output");
		
		String[] testLines = {"!First meta line",
				"!Second meta line",
				"Third line",
				"Fourth line",
				"!Third meta line",
				"Sixth line",
				"Seventh line",
				"Eight line",
				"! Fourth meta line",
				"! Fifth meta line"};
		String tmpFilename = OsPath.join(tmpDir, "unit1.SOFT");		
		FSUtils.writeTextFile(tmpFilename, testLines);
		tfs.putLocalFile(tmpFilename, inputHDFSDir, tmpDir, tmpDir, "gz", 850);
		tmpFilename = OsPath.join(tmpDir, "unit2.SOFT");		
		FSUtils.writeTextFile(tmpFilename, testLines);
		tfs.putLocalFile(tmpFilename, inputHDFSDir, tmpDir, tmpDir, "gz", 850);
		tmpFilename = OsPath.join(tmpDir, "unit3.SOFT");		
		FSUtils.writeTextFile(tmpFilename, testLines);
		tfs.putLocalFile(tmpFilename, inputHDFSDir, tmpDir, tmpDir, "gz", 850);
		tmpFilename = OsPath.join(tmpDir, "unit4.SOFT");		
		FSUtils.writeTextFile(tmpFilename, testLines);
		tfs.putLocalFile(tmpFilename, inputHDFSDir, tmpDir, tmpDir, "gz", 850);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		inputFiles = new ArrayList<String>();
		inputFiles.add(OsPath.join(inputHDFSDir, "unit1.SOFT.850.gz"));
		inputFiles.add(OsPath.join(inputHDFSDir, "unit2.SOFT.850.gz"));
		inputFiles.add(OsPath.join(inputHDFSDir, "unit3.SOFT.850.gz"));
		inputFiles.add(OsPath.join(inputHDFSDir, "unit4.SOFT.850.gz"));
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void executeJob() throws StageException, IOException, TroilkattPropertiesException, StageInitException {	
		MapReduce mrs = new MapReduce(5, "mapreduce-splittest", 
				testJar + " " + testClass,
				hdfsOutput, "none", 100, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);				
		ArrayList<String> outputFiles = mrs.process2(inputFiles, 851);		
		checkOutputFiles(outputFiles, mrs.stageOutputDir, 851);		
	}
	
	// Invalid compression format: Exception should be handled by MapReduce job
	@Test
	public void executeJob2() throws StageException, IOException, TroilkattPropertiesException, StageInitException {		
		MapReduce mrs = new MapReduce(5, "mapreduce-splittest", 
				testJar + " " + testClass,
				hdfsOutput, "none", 100, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);	
		
		inputFiles.add("troilkatt/data/test/mapreduce/input/file6.2.zip");
		ArrayList<String> outputFiles = mrs.process2(inputFiles, 853);		
		checkOutputFiles(outputFiles, mrs.stageOutputDir, 853);
	}
	
	// Non-existing file
	@Test(expected=StageException.class)
	public void executeJob3() throws StageException, IOException, TroilkattPropertiesException, StageInitException {		
		MapReduce mrs = new MapReduce(5, "mapreduce-splittest", 
				testJar + " " + testClass,
				hdfsOutput, "none", 100, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);	
		
		inputFiles.add("troilkatt/data/test/mapreduce/input/does-not-exist.3.gz");
		ArrayList<String> outputFiles = mrs.process2(inputFiles, 854);
		checkOutputFiles(outputFiles, mrs.stageOutputDir, 854);
	}
	
	// Invalid file: Exception should be handled by MapReduce job
	@Test
	public void executeJob4() throws StageException, IOException, TroilkattPropertiesException, StageInitException {		
		MapReduce mrs = new MapReduce(5, "mapreduce-splittest", 
				testJar + " " + testClass,
				hdfsOutput, "none", 100, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);	
		
		inputFiles.add("troilkatt/data/test/mapreduce/input/invalid.5.gz");
		ArrayList<String> outputFiles = mrs.process2(inputFiles, 855);
		checkOutputFiles(outputFiles, mrs.stageOutputDir, 855);
	}
	
	private void checkOutputFiles(ArrayList<String> outputFiles, String localDir, long timestamp) throws IOException {
		assertEquals(4, outputFiles.size());
		
		Collections.sort(outputFiles);
		assertEquals("unit1.SOFT.meta." + timestamp + ".none", OsPath.basename(outputFiles.get(0)));
		assertEquals("unit2.SOFT.meta." + timestamp + ".none", OsPath.basename(outputFiles.get(1)));
		assertEquals("unit3.SOFT.meta." + timestamp + ".none", OsPath.basename(outputFiles.get(2)));
		assertEquals("unit4.SOFT.meta." + timestamp + ".none", OsPath.basename(outputFiles.get(3)));
				
		for (String o: outputFiles) {
			String localName = tfs.getFile(o, localDir, tmpDir, logDir);
			String[] metaLines = FSUtils.readTextFile(localName);			
			assertEquals("!First meta line", metaLines[1]);
			assertEquals("!Third meta line", metaLines[3]);
			assertEquals("! Fifth meta line", metaLines[5]);
		}	
	}
}
