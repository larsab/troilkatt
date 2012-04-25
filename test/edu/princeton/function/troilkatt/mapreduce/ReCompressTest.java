package edu.princeton.function.troilkatt.mapreduce;


import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;

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
import edu.princeton.function.troilkatt.pipeline.MapReduce;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class ReCompressTest extends TestSuper {
	protected static TroilkattFS tfs;
	protected static Pipeline pipeline;	
	protected static TroilkattProperties troilkattProperties;
	protected static String hdfsOutput;
	protected static Logger testLogger;
	
	protected static String testJar = "TROILKATT.JAR";
	protected static String testClass = "edu.princeton.function.troilkatt.mapreduce.ReCompress";

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
		tfs = new TroilkattFS(hdfs);
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs);
		
		localRootDir = tmpDir;
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"),
				OsPath.join("meta", pipeline.name));
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 5, "mapreduce-compresstest"));
		hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"), "tmp");		
		
		hdfsOutput = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"),
				"test/mapreduce/output");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {		
		String inputDir = "troilkatt/data/test/mapreduce/input";
		inputFiles = new ArrayList<String>();
		inputFiles.add(OsPath.join(inputDir, "file1.1.gz"));
		inputFiles.add(OsPath.join(inputDir, "file2.1.gz"));
		inputFiles.add(OsPath.join(inputDir, "file3.1.gz"));
		inputFiles.add(OsPath.join(inputDir, "file4.1.none"));
		inputFiles.add(OsPath.join(inputDir, "file5.2.bz2"));		
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void executeJob() throws StageException, IOException, TroilkattPropertiesException, StageInitException {	
		MapReduce mrs = new MapReduce(5, "mapreduce-compresstest", 
				testJar + " " + testClass,
				hdfsOutput, "gz", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);				
		ArrayList<String> outputFiles = mrs.process2(inputFiles, 841);		
		checkOutputFiles(outputFiles, mrs.stageOutputDir, 841);
		
		mrs = new MapReduce(5, "mapreduce-compresstest", 
				testJar + " " + testClass,
				hdfsOutput, "bz2", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);		
		outputFiles = mrs.process2(inputFiles, 842);		
		checkOutputFiles(outputFiles, mrs.stageOutputDir, 842);
		
		mrs = new MapReduce(5, "mapreduce-compresstest", 
				testJar + " " + testClass,
				hdfsOutput, "none", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);		
		outputFiles = mrs.process2(inputFiles, 843);		
		checkOutputFiles(outputFiles, mrs.stageOutputDir, 843);
	}
	
	// Invalid compression format
	@Test
	public void executeJob2() throws StageException, IOException, TroilkattPropertiesException, StageInitException {		
		MapReduce mrs = new MapReduce(5, "mapreduce-compresstest", 
				testJar + " " + testClass,
				hdfsOutput, "gz", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		inputFiles.add("troilkatt/data/test/mapreduce/input/file6.2.zip");
		ArrayList<String> outputFiles = mrs.process2(inputFiles, 844);
		inputFiles.remove(5);
		checkOutputFiles(outputFiles, mrs.stageOutputDir, 844);
	}
	
	// Non-existing file
	@Test(expected=StageException.class)
	public void executeJob3() throws StageException, IOException, TroilkattPropertiesException, StageInitException {		
		MapReduce mrs = new MapReduce(5, "mapreduce-compresstest", 
				testJar + " " + testClass,
				hdfsOutput, "gz", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		inputFiles.add("troilkatt/data/test/mapreduce/input/does-not-exist.3.gz");
		ArrayList<String> outputFiles = mrs.process2(inputFiles, 845);
		inputFiles.remove(5);
		checkOutputFiles(outputFiles, mrs.stageOutputDir, 845);
	}
	
	// Invalid file
	@Test
	public void executeJob4() throws StageException, IOException, TroilkattPropertiesException, StageInitException {		
		MapReduce mrs = new MapReduce(5, "mapreduce-compresstest", 
				testJar + " " + testClass,
				hdfsOutput, "gz", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		inputFiles.add("troilkatt/data/test/mapreduce/input/invalid.5.gz");
		ArrayList<String> outputFiles = mrs.process2(inputFiles, 846);
		inputFiles.remove(5);
		checkOutputFiles(outputFiles, mrs.stageOutputDir, 846);
	}
	
	private void checkOutputFiles(ArrayList<String> outputFiles, String localDir, long timestamp) throws IOException {
		assertEquals(inputFiles.size(), outputFiles.size());
		
		for (String o: outputFiles) {
			assertTrue(o.contains(String.valueOf(timestamp)));
			
			String localName = tfs.getFile(o, localDir, tmpDir, logDir);
			String basename = OsPath.basename(localName);
			if (basename.equals("file1.1") || basename.equals("file2.1") || basename.equals("file3.1")) {
				fileCmp(localName, OsPath.join(dataDir, "files/file3"));
			}
			else if (basename.equals("file4.1")) {
				fileCmp(localName, OsPath.join(dataDir, "files/file1.4.none"));
			}
			else if (basename.equals("file5.2")) {
				fileCmp(localName, OsPath.join(dataDir, "files/file1"));
			}
			else if (basename.equals("file6.1")) {
				fileCmp(localName, OsPath.join(dataDir, "files/file1"));
			}
			else {
				fail("Invalid basename: " + basename);
			}
			OsPath.delete(localName);
		}	
	}
}
