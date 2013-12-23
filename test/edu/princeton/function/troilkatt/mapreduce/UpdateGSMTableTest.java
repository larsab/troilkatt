package edu.princeton.function.troilkatt.mapreduce;


import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;

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
import edu.princeton.function.troilkatt.fs.LogTableHbase;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;
import edu.princeton.function.troilkatt.hbase.GSMTableSchema;
import edu.princeton.function.troilkatt.hbase.GeoMetaTableSchema;
import edu.princeton.function.troilkatt.pipeline.MapReduce;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class UpdateGSMTableTest extends TestSuper {
	
	protected static TroilkattHDFS tfs;
	protected static Pipeline pipeline;	
	protected static TroilkattProperties troilkattProperties;
	protected static Logger testLogger;
	protected static LogTableHbase lt;
	
	protected static String testJar = "TROILKATT.JAR";
	protected static String testClass = "edu.princeton.function.troilkatt.mapreduce.UpdateGSMTable";

	protected static String localRootDir;
	protected static String hdfsStageMetaDir;
	protected static String hdfsStageTmpDir;

	protected static GeoMetaTableSchema geoMetaSchema;
	protected static HTable geoMetaTable;
	
	protected static GSMTableSchema gsmSchema;
	protected static HTable gsmTable;
	
	protected static byte[] metaFam = Bytes.toBytes("meta");
	protected static byte[] gsmFam = Bytes.toBytes("in");
	
	protected static String hdfsOutput;
	
	// Set before any test is run (once)
	// All tests must use timestamp + <test-number> to ensure that rows are written with proper
	//timestamps
	protected static long timestamp;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		testLogger = Logger.getLogger("test");
		
		troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));
		Configuration hdfsConfig = new Configuration();
		FileSystem hdfs = FileSystem.get(hdfsConfig);			
		tfs = new TroilkattHDFS(hdfs);
		lt = new LogTableHbase("unitPipeline", HBaseConfiguration.create());
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs, lt);
		
		localRootDir = tmpDir;
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"),
				OsPath.join("meta", pipeline.name));
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 5, "mapreduce-metatest"));
		hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "tmp");
		
		Configuration hbConf = HBaseConfiguration.create();
		geoMetaSchema = new GeoMetaTableSchema();
		geoMetaTable = geoMetaSchema.openTable(hbConf, true);
		timestamp = TroilkattStatus.getTimestamp();
		createTestRows(timestamp);
		
		gsmSchema = new GSMTableSchema();
		gsmTable = gsmSchema.openTable(hbConf, true);
		
		
		hdfsOutput = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"),
		"test/mapreduce/output");
		tfs.deleteDir(hdfsOutput);
		tfs.mkdir(hdfsOutput);
		
		
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {

	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void executeJob() throws TroilkattPropertiesException, StageInitException, StageException, IOException {
		MapReduce mrs = new MapReduce(8, "mapreduce-gsmupdate", 
				testJar + " " + testClass + " 2048 4096",
				hdfsOutput, "none", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);				
		
		resetRows(timestamp);
		
		ArrayList<String> inputFiles = new ArrayList<String>();
		ArrayList<String> outputFiles = mrs.process2(inputFiles, timestamp + 1);
		assertTrue(outputFiles.size() > 0);
		
		ArrayList<String> gids = getVals("GSM3120001", "GSE");
		assertEquals(2, gids.size());
		assertTrue(gids.contains("GSE3120001"));
		assertTrue(gids.contains("GSE3120002"));
		
		gids = getVals("GSM3120001", "GDS");
		assertEquals(2, gids.size());
		assertTrue(gids.contains("GDS3120001"));
		assertTrue(gids.contains("GDS3120002"));
		
		gids = getVals("GSM3120010", "GSE");
		assertEquals(3, gids.size());
		assertTrue(gids.contains("GSE3120006"));
		assertTrue(gids.contains("GSE3120008"));
		assertTrue(gids.contains("GSE3120009"));
		
		gids = getVals("GSM3120010", "GDS");
		assertEquals(3, gids.size());
		assertTrue(gids.contains("GDS3120006"));
		assertTrue(gids.contains("GDS3120008"));
		assertTrue(gids.contains("GDS3120009"));		
	}
	
	// Invalid input rows: invalid key, empty sampleIDs column, invalid GSM ID
	// All should be handled by MapReduce job
	@Test
	public void executeJob2() throws IOException, TroilkattPropertiesException, StageInitException, StageException {
		MapReduce mrs = new MapReduce(8, "mapreduce-gsmupdate", 
				testJar + " " + testClass + " 2048 4096",
				hdfsOutput, "none", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		ArrayList<String> inputFiles = new ArrayList<String>();
		
		resetRows(timestamp + 2);
		
		// Row with invalid key		
		addRow("foobarbaz", "Homo sapiens", "GSM3120051", "01 JAN 1970", timestamp + 2);
		Put resetPut = new Put( Bytes.toBytes("GSM3120051"), timestamp + 2);
		resetPut.add(gsmFam, Bytes.toBytes("GSE"), Bytes.toBytes("none"));	
		gsmTable.put(resetPut);
		mrs.process2(inputFiles, timestamp + 3);
		
		ArrayList<String> gids = getVals("GSM3120001", "GSE");
		assertEquals(2, gids.size());
		gids = getVals("GSM3120001", "GDS");
		assertEquals(2, gids.size());
		gids = getVals("GSM3120010", "GSE");
		assertEquals(3, gids.size());		
		gids = getVals("GSM3120051", "GSE");
		assertEquals(1, gids.size());
		assertEquals("none", gids.get(0));
		
		resetRows(timestamp + 4);	
		resetPut = new Put( Bytes.toBytes("GSM3120052"), timestamp + 2);
		resetPut.add(gsmFam, Bytes.toBytes("GSE"), Bytes.toBytes("none"));	
		gsmTable.put(resetPut);
		
		// Row with empty sampleIDs
		addRow("GSE3120052", "Homo sapiens", "", "01 JAN 1970", timestamp + 4);
		mrs.process2(inputFiles, timestamp + 5);
		
		gids = getVals("GSM3120001", "GSE");
		assertEquals(2, gids.size());
		gids = getVals("GSM3120001", "GDS");
		assertEquals(2, gids.size());
		gids = getVals("GSM3120010", "GSE");
		assertEquals(3, gids.size());		
		
		resetRows(timestamp + 6);
		resetPut = new Put( Bytes.toBytes("GSM3120055"), timestamp + 2);
		resetPut.add(gsmFam, Bytes.toBytes("GSE"), Bytes.toBytes("none"));	
		gsmTable.put(resetPut);		

		addRow("GSE3120055", "Homo sapiens", "GSM3120055\nfoo\nGSM3120056\nbar", "01 JAN 1970", timestamp + 6);
		// Row with invalid sampleID
		mrs.process2(inputFiles, timestamp + 7);
		
		gids = getVals("GSM3120001", "GSE");
		assertEquals(2, gids.size());
		gids = getVals("GSM3120001", "GDS");
		assertEquals(2, gids.size());
		gids = getVals("GSM3120010", "GSE");
		assertEquals(3, gids.size());
		gids = getVals("GSM3120055", "GSE");
		assertEquals(1, gids.size());
		assertEquals("GSE3120055", gids.get(0));
	}

	/**
	 * Create test rows in GEO meta table
	 * 
	 * There are 10 series, with
	 * - one overlap: GSE3120001 and GSE3120002
	 * - two supersets: 
	 *     GSE3120003 covers GSE3120004 and GSE3120005
	 *     GSE3120006 covers GSE3120007, GSE3120008, and GSE3120009
	 * - three partial overlaps
	 *     GSE3120010 contains some samples from GSE3120011
	 *     GSE3120012 contains some samples from GSE3120011, GSE3120013 and GSE3120014
	 *     GSE3120015 contains all samples from GSE3120011 and GSE3120013
	 *     
	 * In addition there are corresponding datasets with identical content. These are 
	 * names GDS31200XX
	 * @throws IOException 
	 */
	public static void createTestRows(long timestamp) throws IOException {
		String orgHuman = "Homo sapiens";
		String orgMulti = "Homo sapiens\nMus musculus\nRattus norvegicus";
		String firstDate = "01 JAN 1970";
		String anotherDate = "16 SEP 2011";
		
		// Duplicates
		String gsms = "GSM3120001\nGSM3120002\nGSM3120003";
		addRow("GSE3120001", orgHuman, gsms, firstDate, timestamp);
		addRow("GSE3120002", orgHuman, gsms, anotherDate, timestamp);
		
		addRow("GDS3120001", orgHuman, gsms, firstDate, timestamp);
		addRow("GDS3120002", orgHuman, gsms, anotherDate, timestamp);
		
		// Superset 1
		String supersetGsms = "GSM3120004\nGSM3120005\nGSM3120006\nGSM3120007\nGSM3120008";
		String subsetGsms1 = "GSM3120004\nGSM3120005\nGSM3120006";
		String subsetGsms2 = "GSM3120007\nGSM3120008";
		addRow("GSE3120003", orgHuman, supersetGsms, firstDate, timestamp);
		addRow("GSE3120004", orgHuman, subsetGsms1, firstDate, timestamp);
		addRow("GSE3120005", orgHuman, subsetGsms2, anotherDate, timestamp);
		
		addRow("GDS3120003", orgHuman, supersetGsms, firstDate, timestamp);
		addRow("GDS3120004", orgHuman, subsetGsms1, firstDate, timestamp);
		addRow("GDS3120005", orgHuman, subsetGsms2, anotherDate, timestamp);
		
		// Superset 2
		String supersetGsmsB = "GSM3120009\nGSM3120010\nGSM3120011\nGSM3120012";
		String subsetGsms1B = "GSM3120011";
		String subsetGsms2B = "GSM3120009\nGSM3120012\nGSM3120010";
		String subsetGsms3B = "GSM3120010";
		addRow("GSE3120006", orgMulti, supersetGsmsB, firstDate, timestamp);
		addRow("GSE3120007", orgMulti, subsetGsms1B, firstDate, timestamp);
		addRow("GSE3120008", orgHuman, subsetGsms2B, anotherDate, timestamp);
		addRow("GSE3120009", orgHuman, subsetGsms3B, anotherDate, timestamp);
		
		addRow("GDS3120006", orgMulti, supersetGsmsB, firstDate, timestamp);
		addRow("GDS3120007", orgMulti, subsetGsms1B, firstDate, timestamp);
		addRow("GDS3120008", orgHuman, subsetGsms2B, anotherDate, timestamp);
		addRow("GDS3120009", orgHuman, subsetGsms3B, anotherDate, timestamp);
		
		// Subset 1
		String partialSuperset = "GSM3120013\nGSM3120014\nGSM3120015\nGSM3120016";
		String partialOverlap = "GSM3120013\nGSM3120014";
		addRow("GSE3120010", orgMulti, partialSuperset, firstDate, timestamp);
		addRow("GSE3120011", orgMulti, partialOverlap, firstDate, timestamp);
		
		addRow("GDS3120010", orgMulti, partialSuperset, firstDate, timestamp);
		addRow("GDS3120011", orgMulti, partialOverlap, firstDate, timestamp);
		
		// Subset 2
		String partialSupersetB = "GSM3120013\nGSM3120017\nGSM3120018\nGSM3120019\nGSM3120020";		
		String partialOverlapB2 = "GSM3120017";
		String partialOverlapB3 = "GSM3120019\nGSM3120020";
		addRow("GSE3120012", orgMulti, partialSupersetB, anotherDate, timestamp);
		addRow("GSE3120013", orgHuman, partialOverlapB2, firstDate, timestamp);
		addRow("GSE3120014", orgHuman, partialOverlapB3, firstDate, timestamp);
		
		addRow("GDS3120012", orgMulti, partialSupersetB, anotherDate, timestamp);
		addRow("GDS3120013", orgHuman, partialOverlapB2, firstDate, timestamp);
		addRow("GDS3120014", orgHuman, partialOverlapB3, firstDate, timestamp);
		
		// Subset 3
		String partialSupersetC = "GSM3120013\nGSM3120014\nGSM3120017\nGSM3120021\nGSM3120022";
		addRow("GSE3120015", orgHuman, partialSupersetC, firstDate, timestamp);
		
		addRow("GDS3120015", orgHuman, partialSupersetC, firstDate, timestamp);
	}
	
	/**
	 * Helper function to create and put a row into the GEO meta table
	 * @throws IOException 
	 */
	public static void addRow(String id, String organisms, String sampleIDs, String date, long timestamp) throws IOException {
		Put update = new Put(Bytes.toBytes(id), timestamp);		
		
		update.add(metaFam, Bytes.toBytes("organisms"), Bytes.toBytes(organisms));
		if (sampleIDs != null) {
			update.add(metaFam, Bytes.toBytes("sampleIDs"), Bytes.toBytes(sampleIDs));
		}
		update.add(metaFam, Bytes.toBytes("date"), Bytes.toBytes(date));
		
		geoMetaTable.put(update);
	}
	
	/**
	 * Helper function to delete results from previous run
	 * @throws IOException
	 */
	private void resetRows(long timestamp) throws IOException {
		for (int i = 1; i <= 22; i++) {
			byte[] gsmID = Bytes.toBytes(String.format("GSM31200%02d", i));
			Put put = new Put(gsmID, timestamp);
			put.add(gsmFam, Bytes.toBytes("GSE"), Bytes.toBytes("none"));	
			gsmTable.put(put);
		}
	}
	
	/**
	 * Get column values for a row int the GSM table
	 * 
	 * @param id row ID
	 * @param col column (family is always in)
	 * @return
	 * @throws IOException 
	 */
	private ArrayList<String> getVals(String id, String col) throws IOException {
		Get get = new Get(Bytes.toBytes(id));
		Result result = gsmTable.get(get);
		KeyValue keyVal = result.getColumnLatest(gsmFam, Bytes.toBytes(col));
		assertNotNull(keyVal);
		
		byte[] val = keyVal.getValue();
		assertNotNull(val);
	
		String sval = Bytes.toString(val); 	
		ArrayList<String> vl = new ArrayList<String>();
		String[] parts = sval.split("\n");
		for (String p: parts) {
			vl.add(p.replace("<NEWLINE>", "\n"));
		}
		
		return vl;
	}
}
