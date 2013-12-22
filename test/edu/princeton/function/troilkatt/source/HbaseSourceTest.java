package edu.princeton.function.troilkatt.source;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
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
import edu.princeton.function.troilkatt.fs.LogTableHbase;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class HbaseSourceTest extends TestSuper {
	protected TroilkattHDFS tfs;
	protected LogTableHbase lt;
	protected Pipeline pipeline;
	protected String localRootDir ;
	protected String hdfsStageMetaDir;
	protected String hdfsStageTmpDir;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		TroilkattProperties troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));		
		FileSystem hdfs = FileSystem.get(new Configuration());			
		tfs = new TroilkattHDFS(hdfs);
		lt = new LogTableHbase("unitPipeline", HBaseConfiguration.create());
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs, lt);

		localRootDir = tmpDir;
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), OsPath.join("meta", pipeline.name));
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 0, "geoRawMirror"));
		hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "tmp");
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testHbaseSource() throws TroilkattPropertiesException, StageInitException {
		HbaseSource source = new HbaseSource("hbaseSoruce", 
				"troilkatt-geo-meta meta:organisms334 'Homo sapiens' files:pcl",
				null, null, 0, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);

		assertNotNull(source.table);
		assertNotNull(source.whereColumnFamily);
		assertNotNull(source.whereColumnQualifier);
		assertNotNull(source.wherePattern);
		Matcher matcher = source.wherePattern.matcher("Homo sapiens");
		assertTrue(matcher.find());	
		assertNotNull(source.selectColumnFamily);
		assertNotNull(source.selectColumnQualifier);
	}

	// Invalid tablename
	@Test(expected=StageInitException.class)
	public void testHbaseSource2() throws TroilkattPropertiesException, StageInitException {		
		HbaseSource source = new HbaseSource("hbaseSoruce", 
				"not-a-table meta:organisms334 'Homo sapiens' meta:sampleIDs",
				null, null, 0, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertNull(source);
	}

	// Invalid args
	@Test(expected=StageInitException.class)
	public void testHbaseSource3() throws TroilkattPropertiesException, StageInitException {		
		HbaseSource source = new HbaseSource("hbaseSoruce", 
				"troilkatt-geo-meta metaOrganisms334 'Homo sapiens' meta:sampleIDs",
				null, null, 0, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertNull(source);
	}

	// Invalid args2
	@Test(expected=StageInitException.class)
	public void testHbaseSource4() throws TroilkattPropertiesException, StageInitException {		
		HbaseSource source = new HbaseSource("hbaseSoruce", 
				"troilkatt-geo-meta meta:organisms334 'Homo sapiens' meta:sampleIDs:1",
				null, null, 0, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertNull(source);
	}

	// Invalud args 3
	@Test(expected=StageInitException.class)
	public void testHbaseSource5() throws TroilkattPropertiesException, StageInitException {		
		HbaseSource source = new HbaseSource("hbaseSoruce", 
				"troilkatt-geo-meta meta:organisms334 'Homo sapiens'",
				null, null, 0, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertNull(source);
	}

	@Test
	public void testRetrieve() throws IOException, TroilkattPropertiesException, StageInitException, StageException {		
		HbaseSource source = new HbaseSource("hbaseSoruce", 
				"troilkatt-geo-meta meta:organisms334 'Homo sapiens' files:pcl",
				null, null, 0, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);

		HTable geoMetaTable = source.table;
		assertNotNull(geoMetaTable);
		addRow(geoMetaTable, "GSE3341", "Homo sapiens", "GSM1\nGSM2", "01 JAN 1970", "41.pcl");
		addRow(geoMetaTable, "GSE3342", "Mus musculus", "GSM3\nGSM4", "02 JAN 1970", "42.pcl");
		addRow(geoMetaTable, "GSE3343", "Homo sapiens", "GSM5\nGSM6", "03 JAN 1970", "43.pcl");
		addRow(geoMetaTable, "GSE3344", "Rattus norvegicus", "GSM7\nGSM8", "04 JAN 1970", "44.pcl");
		addRow(geoMetaTable, "GSE3345", "Homo sapiens", "GSM8\nGSM9", "05 JAN 1970", "45.pcl");

		ArrayList<String> metaFiles = new ArrayList<String>();
		ArrayList<String> logFiles = new ArrayList<String>();
		ArrayList<String> outputFiles = source.retrieve(metaFiles, logFiles, 3340);
		assertEquals(3, outputFiles.size());
		Collections.sort(outputFiles);
		assertEquals("41.pcl", outputFiles.get(0));
		assertEquals("43.pcl", outputFiles.get(1));
		assertEquals("45.pcl", outputFiles.get(2));

		source = new HbaseSource("hbaseSoruce", 
				"troilkatt-geo-meta meta:organisms334 'Mus musculus' meta:date",
				null, null, 0, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		outputFiles = source.retrieve(metaFiles, logFiles, 3341);
		assertEquals(1, outputFiles.size());		
		assertEquals("02 JAN 1970", outputFiles.get(0));

		// Invalid organism
		source = new HbaseSource("hbaseSoruce", 
				"troilkatt-geo-meta meta:organisms334 'NotAn organism' meta:sampleIDs",
				null, null, 0, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		outputFiles = source.retrieve(metaFiles, logFiles, 3342);
		assertEquals(0, outputFiles.size());

		
		

		// Invalid where column 2
		source = new HbaseSource("hbaseSoruce", 
				"troilkatt-geo-meta meta:invCol 'NotAn organism' meta:sampleIDs",
				null, null, 0, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		outputFiles = source.retrieve(metaFiles, logFiles, 3344);
		assertEquals(0, outputFiles.size());

		// Invalid select column 2
		source = new HbaseSource("hbaseSoruce", 
				"troilkatt-geo-meta meta:organisms334 'Homo sapiens' files:invCol",
				null, null, 0, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		outputFiles = source.retrieve(metaFiles, logFiles, 3346);
		assertEquals(0, outputFiles.size());
	}
	
	// Invalid where column
	@Test(expected=StageException.class) 
	public void testRetrieveI1() throws TroilkattPropertiesException, StageInitException, StageException {
		HbaseSource source = new HbaseSource("hbaseSoruce", 
				"troilkatt-geo-meta invFam:invCol 'NotAn organism' meta:sampleIDs",
				null, null, 0, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		ArrayList<String> metaFiles = new ArrayList<String>();
		ArrayList<String> logFiles = new ArrayList<String>();
		ArrayList<String> outputFiles = source.retrieve(metaFiles, logFiles, 3343);
		assertEquals(0, outputFiles.size());		
	}
	
	// Invalid select column	
	@Test(expected=StageException.class) 
	public void testRetrieveI2() throws TroilkattPropertiesException, StageInitException, StageException {
		HbaseSource source = new HbaseSource("hbaseSoruce", 
				"troilkatt-geo-meta meta:organisms334 'Homo sapiens' invFam:invCol",
				null, null, 0, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		ArrayList<String> metaFiles = new ArrayList<String>();
		ArrayList<String> logFiles = new ArrayList<String>();
		ArrayList<String> outputFiles = source.retrieve(metaFiles, logFiles, 3345);
		assertEquals(0, outputFiles.size());	
	}

	/**
	 * Helper function to create and put a row into the GEO meta table
	 * @throws IOException 
	 */
	private static void addRow(HTable geoMetaTable, String id, String organisms, String sampleIDs, String date, String filename) throws IOException {
		Put update = new Put(Bytes.toBytes(id));		

		byte[] metaFam = Bytes.toBytes("meta");
		update.add(metaFam, Bytes.toBytes("organisms334"), Bytes.toBytes(organisms));
		update.add(metaFam, Bytes.toBytes("sampleIDs"), Bytes.toBytes(sampleIDs));
		update.add(metaFam, Bytes.toBytes("date"), Bytes.toBytes(date));
		update.add(Bytes.toBytes("files"), Bytes.toBytes("pcl"), Bytes.toBytes(filename));


		geoMetaTable.put(update);
	}
}
