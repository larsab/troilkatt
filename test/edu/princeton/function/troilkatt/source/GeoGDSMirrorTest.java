package edu.princeton.function.troilkatt.source;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.log4j.Logger;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.LogTableHbase;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;
import edu.princeton.function.troilkatt.tools.FilenameUtils;

public class GeoGDSMirrorTest extends TestSuper {
	protected TroilkattProperties troilkattProperties;				
	protected TroilkattHDFS tfs;
	protected LogTableHbase lt;
	protected Pipeline pipeline;
	protected static Logger testLogger;
	
	protected String localRootDir;
	protected String hdfsStageMetaDir;
	protected String hdfsStageTmpDir;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		testLogger = Logger.getLogger("test");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		OsPath.deleteAll(tmpDir);		
	}

	@Before
	public void setUp() throws Exception {		
		troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));		
		FileSystem hdfs = FileSystem.get(new Configuration());			
		tfs = new TroilkattHDFS(hdfs);	
		lt = new LogTableHbase("unitPipeline", HBaseConfiguration.create());
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs, lt);
		
		OsPath.mkdir(tmpDir);
		OsPath.mkdir(logDir);
		OsPath.mkdir(outDir);
		
		localRootDir = tmpDir;
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), OsPath.join("meta", pipeline.name));
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 0, "geoGDSMirror"));
		hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "tmp");
	}

	@After
	public void tearDown() throws Exception {
	}
	
	// 
	@Test
	public void testGeoGDSMirror() throws TroilkattPropertiesException, StageInitException {
		GeoGDSMirror source = new GeoGDSMirror("geoGDSMirror", null,
				"test/geoGDSMirror", "bz2", 3, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertNotNull(source.ftpServer);		
		assertEquals(GeoGDSMirror.GDSftpDir, source.ftpDir);
		assertNotNull(source.adminEmail);
	}
	
	// Not unit tested
	//@Test
	//public void testRetrieve() {
	//}
	
	@Test
	public void testGetOldIDs() throws TroilkattPropertiesException, StageInitException, IOException, StageException {
		GeoGDSMirror source = new GeoGDSMirror("geoGDSMirror", null,
				"test/geoGDSMirror", "bz2", 3, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		// Copy fake GDS files to HDFS output dir
		Path srcFile = new Path(OsPath.join(dataDir, configurationFile));		
		tfs.deleteDir(source.tfsOutputDir);
		tfs.mkdir(source.tfsOutputDir);
		tfs.hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(source.tfsOutputDir, "GDS1.100.bz2")));
		tfs.hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(source.tfsOutputDir, "GDS2.101.bz2")));
		tfs.hdfs.copyFromLocalFile(srcFile, new Path(OsPath.join(source.tfsOutputDir, "GDS3.101.bz2")));
		
		HashSet<String> oldIDs = source.getOldIDs(source.tfsOutputDir);
		assertEquals(3, oldIDs.size());
		assertTrue(oldIDs.contains("GDS1"));
		assertTrue(oldIDs.contains("GDS2"));
		assertTrue(oldIDs.contains("GDS3"));
		
		// Log file not saved in this function
		// assertTrue(OsPath.isfile(OsPath.join(source.stageLogDir, "old")));
	}

	@Test
	public void testGetNewFiles() throws TroilkattPropertiesException, StageInitException, StageException, SocketException, IOException {
		GeoGDSMirror source = new GeoGDSMirror("geoGDSMirror", null,
				"test/geoGDSMirror", "bz2", 3, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		HashSet<String> oldIDs = new HashSet<String>();
		
		FTPClient ftp = new FTPClient();		
	    assertTrue(source.connectFTP(ftp));	    		
		ArrayList<String> newFiles = source.getNewFiles(ftp, oldIDs);
		assertTrue(newFiles.size() > 0);
		
		// Add 50% of files to oldIDs list
		int prevSize = newFiles.size();
		for (int i = 0; i < newFiles.size() / 2; i++) {
			oldIDs.add(FilenameUtils.getDsetID(newFiles.get(i)));
		}
		
		// Log file not saved in this function
		//assertTrue(OsPath.isfile(OsPath.join(source.stageLogDir, "new")));
		
		// Redo the operation
		newFiles = source.getNewFiles(ftp, oldIDs);
		assertTrue(oldIDs.size() + newFiles.size() == prevSize);
		// Make sure no new file is in the old IDs list
		for (String f: newFiles) {
			String gid = FilenameUtils.getDsetID(f);
			assertFalse(oldIDs.contains(gid));
		}
		
		ftp.disconnect();
	}
	
	@Test
	public void testDownloadFile() throws StageException, IOException, TroilkattPropertiesException, StageInitException {
		GeoGDSMirror source = new GeoGDSMirror("geoGDSMirror", null,
				"test/geoGDSMirror", "bz2", 3, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		FTPClient ftp = new FTPClient();		
	    assertTrue(source.connectFTP(ftp));
		String hdfsName = source.downloadFile(ftp, "GDS312.soft.gz", 109);
		ftp.disconnect();
		
		assertEquals(OsPath.join(source.tfsOutputDir, "GDS312.soft.109.bz2"), hdfsName);
		assertTrue(tfs.isfile(hdfsName));
		
		String localName = tfs.getFile(hdfsName, outDir, tmpDir, logDir);
		assertNotNull(localName);
		String sha1 = FSUtils.sha1file(localName);
		assertEquals("f43e4023f9ef2d305c0f2ec0626687c5a8283c39", sha1);
		
		
	}

	@Test
	public void testUnpackAll() throws TroilkattPropertiesException, StageInitException, IOException {
		GeoGDSMirror source = new GeoGDSMirror("geoGDSMirror", null,
				"test/geoGDSMirror", "bz2", 3, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		String packedFile = OsPath.join(dataDir, "dirs/1.tar.gz");
		OsPath.copy(packedFile, OsPath.join(source.stageInputDir, "1.tar.gz"));
		source.unpackAll(source.stageInputDir, source.stageOutputDir);
		
		String[] files = OsPath.listdirR(source.stageOutputDir, testLogger);		
		assertEquals(2, files.length);
		Arrays.sort(files);
		assertTrue(files[0].endsWith("file1"));
		assertTrue(files[1].endsWith("file2"));
	}
}
