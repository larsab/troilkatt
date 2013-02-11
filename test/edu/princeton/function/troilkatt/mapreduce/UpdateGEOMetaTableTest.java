package edu.princeton.function.troilkatt.mapreduce;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
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
import edu.princeton.function.troilkatt.TroilkattStatus;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;
import edu.princeton.function.troilkatt.hbase.GeoMetaTableSchema;
import edu.princeton.function.troilkatt.hbase.HbaseException;
import edu.princeton.function.troilkatt.pipeline.MapReduce;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class UpdateGEOMetaTableTest extends TestSuper {
	protected static TroilkattHDFS tfs;
	protected static Pipeline pipeline;	
	protected static TroilkattProperties troilkattProperties;
	protected static Logger testLogger;
	
	protected static String testJar = "TROILKATT.JAR";
	protected static String testClass = "edu.princeton.function.troilkatt.mapreduce.UpdateGEOMetaTable";

	protected static final String inputDir = "troilkatt/data/test/mapreduce/input";
	protected ArrayList<String> inputFiles;
	
	// Table handle
	protected static GeoMetaTableSchema geoMetaTable;
	protected static HTable table;
	
	protected final byte[] metaFam = Bytes.toBytes("meta");
	
	protected static String localRootDir;
	protected static String hdfsStageMetaDir;
	protected static String hdfsStageTmpDir;
	
	// Set before any test is run (once)
	// All tests must use timestamp + <test-number> to ensure that rows are written with proper
	//timestamps
	protected static long timestamp;
	
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
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 5, "mapreduce-metatest"));
		hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "tmp");		
		
		Configuration hbConf = HBaseConfiguration.create();
		geoMetaTable = new GeoMetaTableSchema();
		try {
			table = geoMetaTable.openTable(hbConf, true);
		} catch (HbaseException e) {
			throw new IOException("HbaseException: " + e.getMessage());
		}
		
		timestamp = TroilkattStatus.getTimestamp();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		inputFiles = new ArrayList<String>();
		
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void executeJob() throws TroilkattPropertiesException, StageInitException, StageException, IOException {
		MapReduce mrs = new MapReduce(5, "mapreduce-metatest", 
				testJar + " " + testClass,
				null, "gz", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);		
		
		resetRows(timestamp);
		
		inputFiles.add(OsPath.join(inputDir, "GDS2949_full.soft.6.gz"));
		inputFiles.add(OsPath.join(inputDir, "GSE8070_family.soft.6.gz"));
		ArrayList<String> outputFiles = mrs.process2(inputFiles, timestamp + 1);
		assertEquals(0, outputFiles.size());
		
		Get get1 = new Get(Bytes.toBytes("GDS2949"));
		Result result1 = table.get(get1);
		verifyGDSRow(result1);
		Get get2 = new Get(Bytes.toBytes("GSE8070"));
		Result result2 = table.get(get2);
		verifyGSERow(result2);
		
		resetRows(timestamp + 2);
		inputFiles.clear();
		inputFiles.add(OsPath.join(inputDir, "GDS2949_full.soft.6.bz2"));
		inputFiles.add(OsPath.join(inputDir, "GSE8070_family.soft.6.bz2"));	
		outputFiles = mrs.process2(inputFiles, timestamp + 3);		
		assertEquals(0, outputFiles.size());
		
		get1 = new Get(Bytes.toBytes("GDS2949"));
		result1 = table.get(get1);
		verifyGDSRow(result1);
		get2 = new Get(Bytes.toBytes("GSE8070"));
		result2 = table.get(get2);
		verifyGSERow(result2);
		
		resetRows(timestamp + 4);
		inputFiles.clear();
		inputFiles.add(OsPath.join(inputDir, "GDS2949_full.soft.6.none"));
		inputFiles.add(OsPath.join(inputDir, "GSE8070_family.soft.6.none"));
		outputFiles = mrs.process2(inputFiles, timestamp + 5);		
		assertEquals(0, outputFiles.size());
		
		get1 = new Get(Bytes.toBytes("GDS2949"));
		result1 = table.get(get1);
		verifyGDSRow(result1);
		get2 = new Get(Bytes.toBytes("GSE8070"));
		result2 = table.get(get2);
		verifyGSERow(result2);
	}
	
	// Not a SOFT file
	@Test
	public void executeJob2() throws TroilkattPropertiesException, StageInitException, StageException, IOException {
		MapReduce mrs = new MapReduce(5, "mapreduce-metatest", 
				testJar + " " + testClass,
				null, "gz", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);				
		
		resetRows(timestamp + 6);
		
		inputFiles.add(OsPath.join(inputDir, "GDS2949_full.soft.6.gz"));
		inputFiles.add(OsPath.join(inputDir, "GSE8070_family.soft.6.gz"));
		inputFiles.add(OsPath.join(inputDir, "file1.1.gz"));
		inputFiles.add(OsPath.join(inputDir, "file2.1.gz"));
		ArrayList<String> outputFiles = mrs.process2(inputFiles, timestamp + 7);		
		assertEquals(0, outputFiles.size());
		
		Get get1 = new Get(Bytes.toBytes("GDS2949"));
		Result result1 = table.get(get1);
		verifyGDSRow(result1);
		Get get2 = new Get(Bytes.toBytes("GSE8070"));
		Result result2 = table.get(get2);
		verifyGSERow(result2);
	}
	
	// Invalid file
	@Test
	public void executeJob3() throws TroilkattPropertiesException, StageInitException, StageException, IOException {
		MapReduce mrs = new MapReduce(5, "mapreduce-metatest", 
				testJar + " " + testClass,
				null, "gz", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);				
		
		resetRows(timestamp + 8);
		
		inputFiles.add(OsPath.join(inputDir, "GDS2949_full.soft.6.gz"));
		inputFiles.add(OsPath.join(inputDir, "GSE8070_family.soft.6.gz"));
		inputFiles.add(OsPath.join(inputDir, "file1.1.gz"));
		inputFiles.add(OsPath.join(inputDir, "invalid.5.gz"));
		ArrayList<String> outputFiles = mrs.process2(inputFiles, timestamp + 9);		
		assertEquals(0, outputFiles.size());
		
		Get get1 = new Get(Bytes.toBytes("GDS2949"));
		Result result1 = table.get(get1);
		verifyGDSRow(result1);
		Get get2 = new Get(Bytes.toBytes("GSE8070"));
		Result result2 = table.get(get2);
		verifyGSERow(result2);
	}
	
	// Non existing file
	@Test(expected=StageException.class)
	public void executeJob4() throws TroilkattPropertiesException, StageInitException, StageException, IOException {
		MapReduce mrs = new MapReduce(5, "mapreduce-metatest", 
				testJar + " " + testClass,
				null, "gz", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);				
		
		resetRows(timestamp + 10);
		
		inputFiles.add(OsPath.join(inputDir, "GDS2949_full.soft.6.gz"));
		inputFiles.add(OsPath.join(inputDir, "GSE8070_family.soft.6.gz"));
		inputFiles.add(OsPath.join(inputDir, "does-not-exist.3.gz"));
		inputFiles.add(OsPath.join(inputDir, "also-does-not-exist.3.gz"));
		ArrayList<String> outputFiles = mrs.process2(inputFiles, timestamp + 11);		
		assertEquals(0, outputFiles.size());
		
		Get get1 = new Get(Bytes.toBytes("GDS2949"));
		Result result1 = table.get(get1);
		verifyGDSRow(result1);
		Get get2 = new Get(Bytes.toBytes("GSE8070"));
		Result result2 = table.get(get2);
		verifyGSERow(result2);
	}
	
	private void resetRows(long timestamp) throws IOException {
		Put resetRow1 = new Put(Bytes.toBytes("GDS2949"), timestamp);
		// Will fail in verify test
		resetRow1.add(Bytes.toBytes("meta"), Bytes.toBytes("date"), Bytes.toBytes("reset"));
		table.put(resetRow1);		
		Put resetRow2 = new Put(Bytes.toBytes("GSE8070"), timestamp);
		// Will fail in verify test
		resetRow2.add(Bytes.toBytes("meta"), Bytes.toBytes("date"), Bytes.toBytes("reset"));
		table.put(resetRow2);
		
		Get get1 = new Get(Bytes.toBytes("GDS2949"));
		Result result1 = table.get(get1);
		assertEquals("reset", getSingleVal(result1, "date"));
		Get get2 = new Get(Bytes.toBytes("GSE8070"));
		Result result2 = table.get(get2);
		assertEquals("reset", getSingleVal(result2, "date"));
	}

	private String getSingleVal(Result result, String col) {
		KeyValue keyVal = result.getColumnLatest(metaFam, Bytes.toBytes(col));
		
		if (keyVal == null) {
			return null;
		}
		
		byte[] val = keyVal.getValue();
		if (val == null) {
			return null;
		}
		
		String rv = Bytes.toString(val); 
		
		return rv;
	}

	private ArrayList<String> getVals(Result result, String col) {
		KeyValue keyVal = result.getColumnLatest(metaFam, Bytes.toBytes(col));
		if (keyVal == null) {
			return null;
		}
		
		byte[] val = keyVal.getValue();
		if (val == null) {
			return null;
		}
		String sval = Bytes.toString(val); 
		
		ArrayList<String> vl = new ArrayList<String>();
		String[] parts = sval.split("\n");
		for (String p: parts) {
			vl.add(p.replace("<NEWLINE>", "\n"));
		}
		
		return vl;
	}

	private void verifyGDSRow(Result result) {
		assertFalse(result.isEmpty());
		
		assertEquals("Pancreatic development (MG-U74B)", getSingleVal(result, "title"));
		assertEquals("Sep 11 2007", getSingleVal(result, "date"));
		assertNull(getSingleVal(result, "pmid"));
		assertEquals("Analysis of pancreatic tissues from NMRI animals from embryonic day E12.5 to E16.5. Results provide insight into the molecular mechanisms underlying the development of the pancreas.", getSingleVal(result, "description"));
		
		ArrayList<String> vals = getVals(result, "organisms");
		assertEquals(1, vals.size());
		assertEquals("Mus musculus", vals.get(0));
		vals = getVals(result, "platformIDs");
		assertEquals(1, vals.size());
		assertEquals("GPL82", vals.get(0));
		vals = getVals(result, "platformTitles");
		assertEquals(1, vals.size());
		assertEquals("in situ oligonucleotide", vals.get(0));
		vals = getVals(result, "rowCounts");
		assertEquals(1, vals.size());
		assertEquals("12477", vals.get(0));
		vals = getVals(result, "sampleIDs");
		int nSamples = vals.size();
		Collections.sort(vals);
		assertEquals(nSamples, vals.size());
		assertEquals("GSM199456", vals.get(3));
		vals = getVals(result, "sampleTitles");
		assertEquals(5, vals.size());
		assertEquals("E13.5", vals.get(1));
		vals = getVals(result, "channelCounts");
		assertEquals(1, vals.size());
		assertEquals("1", vals.get(0));
		vals = getVals(result, "valueTypes");
		assertEquals(1, vals.size());
		assertEquals("transformed count", vals.get(0));
	}

	private void verifyGSERow(Result result) {
		assertFalse(result.isEmpty());
		
		assertEquals("Expression profiling of pancreas development", getSingleVal(result, "title"));
		assertEquals("Dec 10 2010", getSingleVal(result, "date"));
		assertNull(getSingleVal(result, "pmid"));		
		String val = getSingleVal(result, "description");		
		assertTrue(val.startsWith("Development of the pancreas"));
		
		int nPlatforms = 3;
		ArrayList<String> vals = getVals(result, "organisms");
		assertEquals(1, vals.size());
		assertEquals("Mus musculus", vals.get(0));
		vals = getVals(result, "platformIDs");
		assertEquals(nPlatforms, vals.size());
		assertEquals("GPL82", vals.get(1));
		vals = getVals(result, "platformTitles");
		assertEquals(nPlatforms, vals.size());
		assertEquals("[MG_U74Av2] Affymetrix Murine Genome U74 Version 2 Array", vals.get(0));
		vals = getVals(result, "rowCounts");
		assertEquals(nPlatforms, vals.size());
		assertEquals("12477", vals.get(1));
		int nSamples = 30;
		vals = getVals(result, "sampleIDs");
		Collections.sort(vals);
		assertEquals(nSamples, vals.size());
		assertEquals("GSM199446", vals.get(3));
		vals = getVals(result, "sampleTitles");
		assertEquals(nSamples, vals.size());
		assertEquals("Pancreas at e12.5, Chip A, rep 1", vals.get(0));
		vals = getVals(result, "channelCounts");
		assertEquals(1, vals.size());
		assertEquals("1", vals.get(0));
		assertNull(getVals(result, "valueTypes"));			
	}
}
