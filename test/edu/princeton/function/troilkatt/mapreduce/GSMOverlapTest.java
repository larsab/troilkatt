package edu.princeton.function.troilkatt.mapreduce;


import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
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
import edu.princeton.function.troilkatt.hbase.GSMTableSchema;
import edu.princeton.function.troilkatt.hbase.GeoMetaTableSchema;
import edu.princeton.function.troilkatt.hbase.HbaseException;
import edu.princeton.function.troilkatt.pipeline.MapReduce;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

/**
 * Note! executeJob test must have been run in UpdateGSMTableTest before 
 * running these tests
 *
 */
public class GSMOverlapTest extends TestSuper {
	
	protected static TroilkattHDFS tfs;
	protected static Pipeline pipeline;	
	protected static TroilkattProperties troilkattProperties;
	protected static Logger testLogger;
	
	protected static String testJar = "TROILKATT.JAR";
	protected static String testClass = "edu.princeton.function.troilkatt.mapreduce.GSMOverlap";

	protected static String localRootDir;
	protected static String hdfsStageMetaDir;
	protected static String hdfsStageTmpDir;

	protected static GeoMetaTableSchema geoMetaSchema;
	protected static HTable geoMetaTable;
	
	protected static GSMTableSchema gsmSchema;
	protected static HTable gsmTable;
	
	protected static byte[] metaFam = Bytes.toBytes("meta");
	protected static byte[] gsmFam = Bytes.toBytes("in");

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		testLogger = Logger.getLogger("test");
		
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
		geoMetaSchema = new GeoMetaTableSchema();
		geoMetaTable = geoMetaSchema.openTable(hbConf, true);		
		
		gsmSchema = new GSMTableSchema();
		gsmTable = gsmSchema.openTable(hbConf, true);
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
	public void testLoadMeta() throws HbaseException, IOException {
		GSMOverlap.PairCounterReducer reducer = new GSMOverlap.PairCounterReducer();
		
		Configuration hbConf = HBaseConfiguration.create();		
		reducer.geoMetaSchema = new GeoMetaTableSchema();
		reducer.geoMetaTable = reducer.geoMetaSchema.openTable(hbConf, false);
		reducer.gid2nSamples = new HashMap<String, Integer>();
		reducer.gid2meta = new HashMap<String, String>();
		reducer.reduceLogger = testLogger;
		
		assertTrue(reducer.loadMeta("GSE3120001"));		
		assertTrue(reducer.gid2nSamples.containsKey("GSE3120001"));
		int nSamples = reducer.gid2nSamples.get("GSE3120001");
		assertEquals(3, nSamples);
		assertTrue(reducer.gid2meta.containsKey("GSE3120001"));		
		assertEquals("01 JAN 1970\tHomo sapiens", reducer.gid2meta.get("GSE3120001"));
		
		// Retry (should not cause a reload of data)
		assertTrue(reducer.loadMeta("GSE3120001"));		
		assertTrue(reducer.gid2nSamples.containsKey("GSE3120001"));
		nSamples = reducer.gid2nSamples.get("GSE3120001");
		assertEquals(3, nSamples);
		assertTrue(reducer.gid2meta.containsKey("GSE3120001"));		
		assertEquals("01 JAN 1970\tHomo sapiens", reducer.gid2meta.get("GSE3120001"));
		
		// Check GDS
		assertTrue(reducer.loadMeta("GDS3120001"));		
		assertTrue(reducer.gid2nSamples.containsKey("GDS3120001"));
		nSamples = reducer.gid2nSamples.get("GDS3120001");
		assertEquals(3, nSamples);
		assertTrue(reducer.gid2meta.containsKey("GDS3120001"));		
		assertEquals("01 JAN 1970\tHomo sapiens", reducer.gid2meta.get("GDS3120001"));
		
		// Check series with multiple organisms
		assertTrue(reducer.loadMeta("GSE3120007"));		
		assertTrue(reducer.gid2nSamples.containsKey("GSE3120007"));
		nSamples = reducer.gid2nSamples.get("GSE3120007");
		assertEquals(1, nSamples);
		assertTrue(reducer.gid2meta.containsKey("GSE3120007"));		
		assertEquals("01 JAN 1970\tHomo sapiens,Mus musculus,Rattus norvegicus", reducer.gid2meta.get("GSE3120007"));
		
		// Invalid key
		assertFalse(reducer.loadMeta("invalid-key"));

		// No samples in row
		long timestamp = TroilkattStatus.getTimestamp();		
		Put update = new Put(Bytes.toBytes("GSE3120061"), timestamp);		
		update.add(metaFam, Bytes.toBytes("organisms"), Bytes.toBytes("Homo sapiens"));
		update.add(metaFam, Bytes.toBytes("date"), Bytes.toBytes("01 JAN 1970"));
		geoMetaTable.put(update);
		assertFalse(reducer.loadMeta("GSE3120061"));		
	}
	
	@Test
	public void executeJob() throws TroilkattPropertiesException, StageInitException, StageException, IOException {
		Get get = new Get(Bytes.toBytes("GSM3120001"));
		// Check that executeJob in UpdateGSMTableTest has been run
		assertTrue(gsmTable.exists(get));
		
		String hdfsOutput = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"),
				"test/mapreduce/output");
		MapReduce mrs = new MapReduce(9, "mapreduce-gsmoverlap", 
				testJar + " " + testClass,
				hdfsOutput, "bz2", 3, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);				
		
		ArrayList<String> inputFiles = new ArrayList<String>();
		ArrayList<String> outputFiles = mrs.process2(inputFiles, TroilkattStatus.getTimestamp());
		assertTrue(outputFiles.size() > 0);
	}
		
}
