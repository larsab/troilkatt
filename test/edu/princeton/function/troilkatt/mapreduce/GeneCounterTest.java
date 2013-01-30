package edu.princeton.function.troilkatt.mapreduce;


import static org.junit.Assert.*;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
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
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;
import edu.princeton.function.troilkatt.pipeline.MapReduce;
import edu.princeton.function.troilkatt.pipeline.StageException;

public class GeneCounterTest extends TestSuper {
	protected static TroilkattHDFS tfs;
	protected static Pipeline pipeline;	
	protected static TroilkattProperties troilkattProperties;
	protected static String hdfsOutput;
	protected static Logger testLogger;
	
	protected static String testJar = "TROILKATT.JAR";
	protected static String testClass = "edu.princeton.function.troilkatt.mapreduce.GeneCounter";
	protected static String inputDir = "troilkatt/data/test/mapreduce/input";
	
	protected MapReduce mrs;
	protected ArrayList<String> inputFiles;
	protected FileSystem hdfs;
	
	protected static int stageNum = 5;
	protected static String stageName = "mapreduce-genecountertest";
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
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"),
				OsPath.join("meta", pipeline.name));
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", stageNum, stageName));
		hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"), "tmp");	
		
		hdfsOutput = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"),
				"test/mapreduce/output");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		mrs = new MapReduce(stageNum, stageName, 
				testJar + " " + testClass,
				hdfsOutput, "none", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		
		inputFiles = new ArrayList<String>();
		inputFiles.add(OsPath.join(inputDir, "file1.1.gz"));
		inputFiles.add(OsPath.join(inputDir, "file2.1.gz"));
		inputFiles.add(OsPath.join(inputDir, "file3.1.gz"));
		
		// Create file with invalid lines
		Configuration conf = new Configuration();
		hdfs = FileSystem.get(conf);
		String filename = OsPath.join(inputDir, "invalidLines.1.gz");
		Path path = new Path(filename);
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		CompressionCodec outputCodec = factory.getCodec(path);
		OutputStream os = outputCodec.createOutputStream(hdfs.create(path));		
		os.write(Bytes.toBytes("YORF\tNAME\tGWEIGHT\tS1\tS2\tS3\n"));
		os.write(Bytes.toBytes("EWEIGHT\t\t\t1\t1\t1\n"));
		os.write(Bytes.toBytes("GENE1\tGENE1\t1\t1.0\t2.0\t3.0\n"));
		os.write(Bytes.toBytes("GENE2\tALIAS2\t1\t1.1\t2.1\t3.1\n"));     // Mismatching gene names (valid)
		os.write(Bytes.toBytes("GENE3\tGENE3\t1\n"));                     // No expression values (invalid)
		os.write(Bytes.toBytes("\tGENE4\t1\t1.4\t2.4\t3.4\n"));           // Empty gene name 1 (valid)
		os.write(Bytes.toBytes("GENE5\t\t1\t1.5\t2.5\t3.5\n"));           // Empty gene name 2 (valid)
		os.write(Bytes.toBytes("\t\t1\t1.6\t2.6\t3.6\n"));                // Both gene names empty (invalid)
		os.write(Bytes.toBytes("GENE7\tGENE7\t1\t1.7\t\t3.7\n"));         // Some empty expression values (valid)
		os.write(Bytes.toBytes("GENE8\tGENE8\t1\t1.8\t2.8\t3.8\t3.9\n")); // Additional expression values (valid)				
		IOUtils.closeStream(os);	
		inputFiles.add(filename);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void executeJob() throws StageException, IOException {
		ArrayList<String> outputFiles = mrs.process2(inputFiles, 832);		
		assertFalse(outputFiles.isEmpty());		
		
		int genesChecked = 0;
		boolean gene3found = false;
		boolean gene6found = false;
		
		for (String s: outputFiles) {
			String basename = OsPath.basename(s);
			if (basename.startsWith("part-r-")) {
				String tmpFilename = OsPath.join(tmpDir, basename);
				System.err.println("Write results file to: " + tmpFilename);
				hdfs.copyToLocalFile(new Path(s), new Path(tmpFilename));
				String[] lines = FSUtils.readTextFile(tmpFilename);				
				
				for (String l: lines) {
					String[] cols = l.split("\t");
					if (cols.length != 2) {
						System.err.println("Invalid line in results file: " + l);
						fail("Invalid results file");
					}
					String geneName = cols[0];
					int count = Integer.valueOf(cols[1]);					
					
					if (geneName.equals("A2M")) {
						assertEquals(3, count);
						genesChecked++;
					}
					else if (geneName.equals("SLC9A6")) {
						assertEquals(3, count);
						genesChecked++;
					}
					else if (geneName.equals("GENE1")) {
						assertEquals(1, count);
						genesChecked++;
					}
					else if (geneName.equals("GENE2")) {
						assertEquals(1, count);
						genesChecked++;
					}
					else if (geneName.equals("GENE3")) {
						gene3found = true;						
					}
					else if (geneName.equals("GENE4")) {
						assertEquals(1, count);
						genesChecked++;
					}
					else if (geneName.equals("GENE5")) {
						assertEquals(1, count);
						genesChecked++;
					}
					else if (geneName.equals("GENE6")) {
						gene6found = true;						
					}
					else if (geneName.equals("GENE7")) {
						assertEquals(1, count);
						genesChecked++;
					}
					else if (geneName.equals("GENE8")) {
						assertEquals(1, count);
						genesChecked++;
					}
				}
			}
		}
		assertFalse(gene3found);
		assertFalse(gene6found);
		assertEquals(8, genesChecked);
	}
	
	// No input files
	@Test
	public void executeJob2() throws StageException {
		inputFiles.clear();
		ArrayList<String> outputFiles = mrs.process2(inputFiles, 833);		
		assertEquals(0, outputFiles.size());				
	}
	
	// Non-existing input file
	@Test(expected=StageException.class)
	public void executeJob3() throws StageException {
		inputFiles.add("/non/existing/input/file1.5.gz");
		ArrayList<String> outputFiles = mrs.process2(inputFiles, 834);		
		assertEquals(0, outputFiles.size());				
	}
	
	// Invalid input file
	@Test(expected=StageException.class)
	public void executeJob4() throws StageException, IOException {
		// Copy invalid input file
		String invalidFilename = OsPath.join(inputDir, "invalid.5.gz");
		hdfs.copyFromLocalFile(new Path(OsPath.join(dataDir, "files/invalid.5.gz")),
				new Path(invalidFilename));		
		inputFiles.add(invalidFilename);
		ArrayList<String> outputFiles = mrs.process2(inputFiles, 834);		
		assertEquals(0, outputFiles.size());				
	}
}
