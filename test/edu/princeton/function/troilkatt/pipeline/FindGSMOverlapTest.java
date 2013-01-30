package edu.princeton.function.troilkatt.pipeline;

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
import edu.princeton.function.troilkatt.TroilkattStatus;
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;
import edu.princeton.function.troilkatt.hbase.GeoMetaTableSchema;
import edu.princeton.function.troilkatt.hbase.TroilkattTable;

public class FindGSMOverlapTest extends TestSuper {
	protected static TroilkattProperties troilkattProperties;				
	protected static TroilkattHDFS tfs;
	protected static Pipeline pipeline;
	
	protected static Logger testLogger;
	protected static String cmd;
	
	protected static String localRootDir;
	protected static String hdfsStageMetaDir;
	protected static String hdfsStageTmpDir;
	
	protected static String idListFilename; 
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		cmd = "python " + OsPath.join(dataDir, "bin/executeDirTest.py") + "TROILKATT.INPUT_DIR TROILKATT.OUTPUT_DIR TROILKATT.META_DIR/filelist > TROILKATT.LOG_DIR/executeSourceTest.out 2> TROILKATT.LOG_DIR/executeSoruceTest.log ";
		testLogger = Logger.getLogger("test");
		
		troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));		
		FileSystem hdfs = FileSystem.get(new Configuration());			
		tfs = new TroilkattHDFS(hdfs);
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs);
		
		localRootDir = tmpDir;		
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"), OsPath.join("meta", pipeline.name));
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 5, "filter"));
		hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"), "tmp");
		
		idListFilename = OsPath.join(tmpDir, "ids");
	}


	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {		
		String[] ids = {"GSE1\n", "GSE2\n", "GSE3\n"};
		FSUtils.writeTextFile(idListFilename, ids);
	}

	@After
	public void tearDown() throws Exception {
	}

	// Not tested since this function is mostly tested as part of the unit tests for 
	// 
	//@Test
	//public void testProcess() {
	//}

	
	@Test
	public void testFindGSMOverlap() throws TroilkattPropertiesException, StageInitException {				
		FindGSMOverlap stage = new FindGSMOverlap(5, "findGsmOverlap", 
				"overlap.deleted 3 3 " + idListFilename,
				"test/gsm-overlap", "bz2", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertNotNull(stage);
	}
	
	// Invalid number of arguments
	@Test(expected=StageInitException.class)
	public void testFindGSMOverlap2() throws TroilkattPropertiesException, StageInitException {
		FindGSMOverlap stage = new FindGSMOverlap(5, "findGsmOverlap", "overlap.deleted 3 3",
				"test/gsm-overlap", "bz2", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertNotNull(stage);		
	}

	// Invalid minSamples
	@Test(expected=StageInitException.class)
	public void testFindGSMOverlap3() throws TroilkattPropertiesException, StageInitException {
		FindGSMOverlap stage = new FindGSMOverlap(5, "findGsmOverlap", 
				"overlap.deleted foo 3 " + idListFilename,
				"test/gsm-overlap", "bz2", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertNotNull(stage);		
	}
	
	// Invalid maxOverlap
	@Test(expected=StageInitException.class)
	public void testFindGSMOverlap4() throws TroilkattPropertiesException, StageInitException {
		FindGSMOverlap stage = new FindGSMOverlap(5, "findGsmOverlap", 
				"overlap.deleted 3 foo "  + idListFilename,
				"test/gsm-overlap", "bz2", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertNotNull(stage);		
	}
	
	// Invalid list filename
	@Test(expected=StageInitException.class)
	public void testFindGSMOverlap5() throws TroilkattPropertiesException, StageInitException {
		FindGSMOverlap stage = new FindGSMOverlap(5, "findGsmOverlap", 
				"overlap.deleted 3 foo "  + "/not/a-file",
				"test/gsm-overlap", "bz2", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertNotNull(stage);		
	}
	
	@Test
	public void testPostSamples() throws TroilkattPropertiesException, StageInitException, IOException {
		FindGSMOverlap stage = new FindGSMOverlap(5, "findGsmOverlap", 
				"overlap.deleted 3 3 "  + idListFilename,
				"test/gsm-overlap", "bz2", -1, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		ArrayList<String> putSamples = new ArrayList<String>();
		putSamples.add("GSM4455661");
		putSamples.add("GSM4455662");
		putSamples.add("GSM4455663");
				
		String gid = "GSE445566";
		long timestamp = TroilkattStatus.getTimestamp();
		stage.putPostSamples(gid, putSamples, timestamp);
		
		String val = GeoMetaTableSchema.getValue(stage.metaTable, gid, "calculated", "sampleIDs-overlapRemoved", stage.logger);
		assertNotNull(val);
		ArrayList<String>  getSamples = TroilkattTable.string2array(val);
		assertEquals(putSamples.size(), getSamples.size());
		for (String s: putSamples) {
			assertTrue(getSamples.contains(s));
		}
		
		// Empty list
		putSamples.clear();
		gid = "GSE445567"; 
		stage.putPostSamples(gid, putSamples, timestamp);		
		val = GeoMetaTableSchema.getValue(stage.metaTable, gid, "calculated", "sampleIDs-overlapRemoved", stage.logger);
		assertNotNull(val);
		getSamples = TroilkattTable.string2array(val);
		assertEquals(1, getSamples.size());
		assertEquals("none", getSamples.get(0));
	}
}
