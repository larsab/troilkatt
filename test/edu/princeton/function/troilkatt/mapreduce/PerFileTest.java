package edu.princeton.function.troilkatt.mapreduce;

import static org.junit.Assert.*;

import java.io.BufferedReader;
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
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFS;
import edu.princeton.function.troilkatt.pipeline.MapReduce;
import edu.princeton.function.troilkatt.pipeline.StageException;

public class PerFileTest extends TestSuper {
	protected static TroilkattFS tfs;
	protected static Pipeline pipeline;	
	protected static TroilkattProperties troilkattProperties;
	protected static String hdfsOutput;
	protected static Logger testLogger;
	
	protected static String testJar = "TROILKATT.JAR";
	protected static String testClass = "edu.princeton.function.troilkatt.mapreduce.PerFileUnitTest";
	protected static String inputDir = "troilkatt/data/test/mapreduce/input";
	
	protected MapReduce mrs;
	protected ArrayList<String> inputFiles;
	protected PerFile.PerFileMapper pfm;
	
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
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 5, "mapreduce-perfiletest"));
		hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"), "tmp");				
		
		hdfsOutput = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"),
				"test/mapreduce/output");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		mrs = new MapReduce(5, "mapreduce-perfiletest", 
				testJar + " " + testClass,
				hdfsOutput, "gz", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
				
		inputFiles = new ArrayList<String>();
		inputFiles.add(OsPath.join(inputDir, "file1.1.gz"));
		inputFiles.add(OsPath.join(inputDir, "file2.1.gz"));
		inputFiles.add(OsPath.join(inputDir, "file3.1.gz"));		
		
		pfm = new PerFile.PerFileMapper();	
		pfm.conf = new Configuration();
		pfm.hdfs = FileSystem.get(pfm.conf);
		pfm.tfs = new TroilkattFS(pfm.hdfs);
		pfm.taskInputDir = tmpDir;
		pfm.taskTmpDir = tmpDir;
		pfm.taskLogDir = tmpDir;
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void executeJob() throws StageException {		
		ArrayList<String> outputFiles = mrs.process2(inputFiles, 831);		
		assertEquals(3, outputFiles.size());		
		
		assertEquals(inputFiles.size(), outputFiles.size());
		for (String fi: outputFiles) {
			String basename = OsPath.basename(fi);
			boolean fileFound = false;
			for (String fo: outputFiles) {
				if (basename.equals(OsPath.basename(fo))) {
					fileFound = true;
					break;
				}				
			}
			assertTrue(fileFound);
		}
	}
	
	@Test
	public void testOpenBufferedReader() throws IOException {		
		BufferedReader lr = pfm.openBufferedReader(OsPath.join(inputDir, "file1.1.gz"));		
		String firstLine = lr.readLine();
		assertNotNull(firstLine);
		assertTrue(firstLine.startsWith("YORF\tNAME\tGWEIGHT"));
		assertTrue(firstLine.endsWith("dioxin-treated"));
		
		lr = pfm.openBufferedReader(OsPath.join(inputDir, "file5.2.bz2"));		
		firstLine = lr.readLine();
		assertNotNull(firstLine);
		assertTrue(firstLine.startsWith("10116\t24152"));
		assertTrue(firstLine.endsWith("831"));		
		
		lr = pfm.openBufferedReader(OsPath.join(inputDir, "file4.1.none"));
		firstLine = lr.readLine();
		assertNotNull(firstLine);
		assertTrue(firstLine.startsWith("10116\t24152"));
		assertTrue(firstLine.endsWith("831"));			
	}
	
	// Invalid compression format
	@Test
	public void testOpenLineReader2() throws IOException {
		BufferedReader lr = pfm.openBufferedReader(OsPath.join(inputDir, "file6.2.zip"));
		assertNull(lr);		
	}
	
	// Non-existing file
	@Test
	public void testOpenLineReader3() throws IOException {
		BufferedReader lr = pfm.openBufferedReader(OsPath.join(inputDir, "does-not-exist.3.gz"));
		assertNull(lr);		
	}
	
	// Non-existing file 2
	@Test
	public void testOpenLineReader4() throws IOException {
		BufferedReader lr = pfm.openBufferedReader(OsPath.join(inputDir, "does-not-exist.3.zip"));
		assertNull(lr);		
	}
	
	// Non-existing file 3
	@Test
	public void testOpenLineReader5() throws IOException {
		BufferedReader lr = pfm.openBufferedReader(OsPath.join(inputDir, "does-not-exist.3.none"));
		assertNull(lr);		
	}
	
	// Invalid file
	@Test
	public void testOpenLineReader6() throws IOException {
		BufferedReader lr = pfm.openBufferedReader(OsPath.join(inputDir, "invalid.5.gz"));
		assertNull(lr);
		//lr.readLine();		
	}
}
