package edu.princeton.function.troilkatt.mapreduce;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.princeton.function.troilkatt.TestSuper;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.hbase.GeoMetaTableSchema;
import edu.princeton.function.troilkatt.pipeline.Stage;

public class RawRemoveOverlappingTest extends TestSuper {	
	protected static HTable metaTable;
	protected static Logger testLogger;
	protected BatchRawRemoveOverlapping.SplitRemoveOverlapMapper mapper;
	
	protected String tarFilename;
	protected String myLogDir;
	protected String myInputDir;
	protected String myOutputDir;
	protected String myTmpDir;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		Configuration hbConf = HBaseConfiguration.create();
		GeoMetaTableSchema schema = new GeoMetaTableSchema();
		metaTable = schema.openTable(hbConf, true);
		
		testLogger = Logger.getLogger("test");
		
		String seriesID = "GSE9999999";
		if (GeoMetaTableSchema.getValue(metaTable, seriesID + "-GPL8888888", "meta", "id", testLogger) == null) {
			// Add necessary rows to meta-data table
			Put update = new Put(Bytes.toBytes(seriesID + "-GPL8888888"), 31279);			
			update.add(Bytes.toBytes("meta"), Bytes.toBytes("id"), Bytes.toBytes(seriesID + "-GPL8888888"));
			update.add(Bytes.toBytes("calculated"), Bytes.toBytes("sampleIDs-overlapRemoved"), Bytes.toBytes("GSM777777777\nGSM777777778\nGSM777777779"));
			metaTable.put(update);
			
			update = new Put(Bytes.toBytes(seriesID + "-GPL8888889"), 31279);			
			update.add(Bytes.toBytes("meta"), Bytes.toBytes("id"), Bytes.toBytes(seriesID + "-GPL8888889"));
			update.add(Bytes.toBytes("calculated"), Bytes.toBytes("sampleIDs-overlapRemoved"), Bytes.toBytes("GSM777777780\nGSM777777781\nGSM777777782"));
			metaTable.put(update);
			
			update = new Put(Bytes.toBytes(seriesID + "-GPL8888890"), 31279);			
			update.add(Bytes.toBytes("meta"), Bytes.toBytes("id"), Bytes.toBytes(seriesID + "-GPL8888890"));
			update.add(Bytes.toBytes("calculated"), Bytes.toBytes("sampleIDs-overlapRemoved"), Bytes.toBytes("none"));
			metaTable.put(update);
		}
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		mapper = new BatchRawRemoveOverlapping.SplitRemoveOverlapMapper();
		mapper.mapLogger = testLogger;
		mapper.metaTable = metaTable;
		
		tarFilename = OsPath.join(dataDir, "files/GSE9999999_RAW.tar");
		myLogDir = OsPath.join(tmpDir, "log");
		myTmpDir = OsPath.join(tmpDir, "tmp");
		myInputDir = OsPath.join(tmpDir, "input");
		myOutputDir = OsPath.join(tmpDir, "output");
		OsPath.deleteAll(myLogDir);
		OsPath.deleteAll(myTmpDir);
		OsPath.deleteAll(myInputDir);
		OsPath.deleteAll(myOutputDir);
		OsPath.mkdir(myLogDir);
		OsPath.mkdir(myTmpDir);
		OsPath.mkdir(myInputDir);
		OsPath.mkdir(myOutputDir);
		mapper.taskInputDir = myInputDir;
		mapper.taskOutputDir = myOutputDir;
		mapper.taskLogDir = myLogDir;		
	}

	@After
	public void tearDown() throws Exception {
		OsPath.deleteAll(myLogDir);
		OsPath.deleteAll(myTmpDir);
		OsPath.deleteAll(myInputDir);
		OsPath.deleteAll(myOutputDir);
	}

	@Test
	public void testGetParts() {
		String seriesID = "GSE9999999";
		HashMap<String, ArrayList<String>> s2g = mapper.getParts(seriesID);
		assertEquals(3, s2g.size());
		Set<String> keys = s2g.keySet();
		assertTrue(keys.contains( seriesID + "-GPL8888888") );
		assertTrue(keys.contains( seriesID + "-GPL8888889") );
		assertTrue(keys.contains( seriesID + "-GPL8888890") );
		
		ArrayList<String> gsms1 = s2g.get(seriesID + "-GPL8888888");
		assertEquals(3, gsms1.size());
		assertEquals("GSM777777777", gsms1.get(0));
		assertEquals("GSM777777778", gsms1.get(1));
		assertEquals("GSM777777779", gsms1.get(2));
		
		ArrayList<String> gsms2 = s2g.get(seriesID + "-GPL8888889");
		assertEquals(3, gsms2.size());
		assertEquals("GSM777777780", gsms2.get(0));
		assertEquals("GSM777777781", gsms2.get(1));
		assertEquals("GSM777777782", gsms2.get(2));
		
		ArrayList<String> gsms3 = s2g.get(seriesID + "-GPL8888890");
		assertEquals(1, gsms3.size());
		assertEquals("none", gsms3.get(0));
		
		// Non-existing, but still valid ID
		s2g = mapper.getParts("GSE66666666");
		assertEquals(0, s2g.size());
		
		// Invalid ID
		s2g = mapper.getParts("invalid-ID");
		assertEquals(0, s2g.size());
	}
	
	@Test
	public void testUntarRaw() {
		int rv = mapper.untarRaw(tarFilename);
		assertEquals(0, rv);
		
		String[] files = OsPath.listdir(myInputDir);
		assertEquals(13, files.length);
		Arrays.sort(files);
		assertEquals("GSM777777777.CEL.gz", OsPath.basename(files[0]));
		assertEquals("foo.gz", OsPath.basename(files[12]));
		
		// Invalid tar file
		rv = mapper.untarRaw("invalid.tar");
		assertTrue(rv != 0);		
	}
	
	@Test
	public void testGetRawFiles() {
		int rv = mapper.untarRaw(tarFilename);		
		assertEquals(0, rv);

		ArrayList<String> rawFiles = mapper.getRawFiles("GSM777777777"); 
		assertEquals(1, rawFiles.size());
		assertEquals("GSM777777777.CEL.gz", OsPath.basename(rawFiles.get(0)));
		
		rawFiles = mapper.getRawFiles("GSM777777778");		                               
		assertEquals(1, rawFiles.size());
		assertEquals("GSM777777778.CEL.gz", OsPath.basename(rawFiles.get(0)));
		
		rawFiles = mapper.getRawFiles("GSM777777781");
		assertEquals(1, rawFiles.size());
		assertEquals("GSM777777781.cel.gz", OsPath.basename(rawFiles.get(0)));
		
		rawFiles = mapper.getRawFiles("GSM777777782");
		assertEquals(1, rawFiles.size());
		assertEquals("GSM777777782.Cel.gz", OsPath.basename(rawFiles.get(0)));
		
		// Non-existing GSM id
		rawFiles = mapper.getRawFiles("GSM777777666");
		assertEquals(0, rawFiles.size());
	}
	
	@Test
	public void testSplitCelFiles() {
		int rv = mapper.untarRaw(tarFilename);
		assertEquals(0, rv);
		
		String seriesID = "GSE9999999";
		String subset1 = seriesID + "-GPL8888888";
		String subset2 = seriesID + "-GPL8888889";
		String subset3 = seriesID + "-GPL8888890";
		HashMap<String, ArrayList<String>> s2g = mapper.getParts(seriesID);
		assertEquals(3, s2g.size());
		
		int added = mapper.splitCelFiles(subset1, s2g.get(subset1));
		assertEquals(3, added);
		String outputTar = OsPath.join(myOutputDir, subset1 + ".tar");
		assertTrue(OsPath.isfile(outputTar));
		OsPath.deleteAll(myTmpDir);
		OsPath.mkdir(myTmpDir);
		assertEquals(0, Stage.executeCmd("tar xvf " + outputTar + " -C " + myTmpDir, testLogger));
		String[] files = OsPath.listdir(myTmpDir);
		Arrays.sort(files);
		assertEquals(3, files.length);
		assertEquals("GSM777777777.CEL.gz", OsPath.basename(files[0]));
		assertEquals("GSM777777778.CEL.gz", OsPath.basename(files[1]));
		assertEquals("GSM777777779.CEL.gz", OsPath.basename(files[2]));
		
		added = mapper.splitCelFiles(subset2, s2g.get(subset2));
		assertEquals(3, added);
		outputTar = OsPath.join(myOutputDir, subset2 + ".tar");
		assertTrue(OsPath.isfile(outputTar));
		OsPath.deleteAll(myTmpDir);
		OsPath.mkdir(myTmpDir);
		assertEquals(0, Stage.executeCmd("tar xvf " + outputTar + " -C " + myTmpDir, testLogger));
		files = OsPath.listdir(myTmpDir);
		Arrays.sort(files);
		assertEquals(3, files.length);
		assertEquals("GSM777777780.CEL.gz", OsPath.basename(files[0]));
		assertEquals("GSM777777781.cel.gz", OsPath.basename(files[1]));
		assertEquals("GSM777777782.Cel.gz", OsPath.basename(files[2]));
		
		// Subset with all samples deleted
		added = mapper.splitCelFiles(subset3, s2g.get(subset3));
		assertEquals(0, added);
		outputTar = OsPath.join(myOutputDir, subset3 + ".tar");
		assertFalse(OsPath.isfile(outputTar));
		
		// Subset with empty samples list
		added = mapper.splitCelFiles(subset3, new ArrayList<String>());
		assertEquals(0, added);
		outputTar = OsPath.join(myOutputDir, subset3 + ".tar");
		assertFalse(OsPath.isfile(outputTar));
		
		// Subset with one non-existing samples
		ArrayList<String> gsms = s2g.get(subset2);
		gsms.add("GSM777777766");
		gsms.add("GSM777777767");
		added = mapper.splitCelFiles(subset2, gsms);
		assertEquals(3, added);
	}
}
