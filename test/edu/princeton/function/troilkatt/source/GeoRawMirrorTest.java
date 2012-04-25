package edu.princeton.function.troilkatt.source;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.SocketException;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.PipelineException;
import edu.princeton.function.troilkatt.TestSuper;
import edu.princeton.function.troilkatt.Troilkatt;
import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFS;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class GeoRawMirrorTest extends TestSuper {
	protected TroilkattFS tfs;
	protected GeoRawMirror source;
	
	
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
		tfs = new TroilkattFS(hdfs);
		Pipeline pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs);
		
		String localRootDir = tmpDir;
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"), OsPath.join("meta", pipeline.name));
		String hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 0, "geoRawMirror"));
		String hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"), "tmp");
		
		source = new GeoRawMirror("geoRawMirror", "'Homo sapiens'",
				"test/geoRawMirror", "none", 3, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGeoRawMirror() throws IOException, TroilkattPropertiesException, StageInitException, PipelineException {				
		assertNotNull(source.ftpServer);		
		assertEquals(GeoRawMirror.rawFtpDir, source.ftpDir);
		assertNotNull(source.adminEmail);
	}
	
	@Test
	public void testDownloadRawFile() throws SocketException, IOException {
		FTPClient ftp = new FTPClient();		
		assertTrue(source.connectFTP(ftp));
		
		String ftpFilename = OsPath.join(source.ftpDir, "GSE31278/GSE31278_RAW.tar");
		String localFilename = OsPath.join(tmpDir, "GSE32178_RAW.tar");
		assertTrue(GeoRawMirror.downloadRawFile(ftp, ftpFilename, localFilename, source.logger));		
		assertTrue(OsPath.isfile(localFilename));
		String sha1 = FSUtils.sha1file(localFilename);
		assertEquals("22a8efb67c3d4f71ed6f2704ee7fc0a7b0dd54eb", sha1);

		ftp.disconnect();
	}

	// Retrieve is not unit tested
	// @Test
	// public void testRetrieve() {
	//}

}
