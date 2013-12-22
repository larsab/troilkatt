package edu.princeton.function.troilkatt.source;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.LogTableHbase;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;
import edu.princeton.function.troilkatt.pipeline.StageException;

public class GeoRawOrgTest extends TestSuper {
	protected TroilkattHDFS tfs;
	protected LogTableHbase lt;
	protected GeoRawOrg source;

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
		Pipeline pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs, lt);
		
		String localRootDir = tmpDir;
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), OsPath.join("meta", pipeline.name));
		String hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 0, "geoRawMirror"));
		String hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "tmp");
		
		source = new GeoRawOrg("geoRawOrgMirror",
				"'Homo sapiens'",
				"test/geoRawOrgMirror", "none", 3, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testConstructor() {
		assertNotNull(source.metaTable);
		assertNotNull(source.orgPattern);
		
		Matcher matcher = source.orgPattern.matcher("Homo sapiens");
		assertTrue(matcher.find());											 
	}
	
	@Test
	public void testReadMetaFile() throws IOException, StageException {
		String metaFile = OsPath.join(source.stageMetaDir, "filelist");	
		String[] metaLines = {
				"GSE1", 
				"GSE2", 
				"GSE3"};		
		
		FSUtils.writeTextFile(metaFile, metaLines);		
		ArrayList<String> metaFiles = new ArrayList<String>();
		metaFiles.add(metaFile);
		
		HashSet<String> prevFiles = source.readMetaFile(metaFiles, "filelist");
		assertEquals(3, prevFiles.size());
		assertTrue(prevFiles.contains("GSE1"));
		assertTrue(prevFiles.contains("GSE2"));
		assertTrue(prevFiles.contains("GSE3"));
		
		metaLines = new String[0];
		FSUtils.writeTextFile(metaFile, metaLines);		
		metaFiles = new ArrayList<String>();
		metaFiles.add(metaFile);
		
		prevFiles = source.readMetaFile(metaFiles, "filelist");
		assertEquals(0, prevFiles.size());
	}
	
	// Invalid metafile
	@Test(expected=StageException.class)
	public void testReadMetaFile2() throws StageException, IOException {				
		String metaFile = OsPath.join(source.stageMetaDir, "invalidNAme");	
		String[] metaLines = {
				"GSE1", 
				"GSE2", 
				"GSE3"};		

		FSUtils.writeTextFile(metaFile, metaLines);		
		ArrayList<String> metaFiles = new ArrayList<String>();
		metaFiles.add(metaFile);

		HashSet<String> prevFiles = source.readMetaFile(metaFiles, metaFile);
		assertEquals(3, prevFiles.size());
	}
		
	@Test
	public void testGetOrgIDs() throws IOException, StageException {
		HTable geoMetaTable = source.metaTable;
		assertNotNull(geoMetaTable);
		addRow(geoMetaTable, "GSE3331", "Homo sapiens", "GSM1\nGSM2", "01 JAN 1970");
		addRow(geoMetaTable, "GSE3332", "Mus musculus", "GSM3\nGSM4", "02 JAN 1970");
		addRow(geoMetaTable, "GSE3333", "Homo sapiens", "GSM5\nGSM6", "03 JAN 1970");
		addRow(geoMetaTable, "GSE3334", "Rattus norvegicus", "GSM7\nGSM8", "04 JAN 1970");
		addRow(geoMetaTable, "GSE3335", "Homo sapiens", "GSM8\nGSM9", "05 JAN 1970");
		
		HashSet<String> oldIDs = source.getOrgIDs();
		assertTrue(oldIDs.contains("GSE3331"));
		assertTrue(oldIDs.contains("GSE3333"));
		assertTrue(oldIDs.contains("GSE3335"));
		
		// Change organism in source object
		source.orgPattern = Pattern.compile("Mus musculus");
		oldIDs = source.getOrgIDs();
		assertTrue(oldIDs.contains("GSE3332"));
		
		// Change again
		source.orgPattern = Pattern.compile("Not a single organism should match this");
		oldIDs = source.getOrgIDs();
		assertTrue(oldIDs.isEmpty());
	}
	
	/**
	 * Helper function to create and put a row into the GEO meta table
	 * @throws IOException 
	 */
	private static void addRow(HTable geoMetaTable, String id, String organisms, String sampleIDs, String date) throws IOException {
		Put update = new Put(Bytes.toBytes(id));		
		
		byte[] metaFam = Bytes.toBytes("meta");
		update.add(metaFam, Bytes.toBytes("organisms"), Bytes.toBytes(organisms));
		update.add(metaFam, Bytes.toBytes("sampleIDs"), Bytes.toBytes(sampleIDs));
		update.add(metaFam, Bytes.toBytes("date"), Bytes.toBytes(date));
		
		geoMetaTable.put(update);
	}
	
	// Not unit tested
	//@Test
	//public void testRetrieve() {
	//	
	//}

}
