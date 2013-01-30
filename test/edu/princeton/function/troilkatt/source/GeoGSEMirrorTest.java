package edu.princeton.function.troilkatt.source;

import static org.junit.Assert.*;

import java.io.IOException;

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
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class GeoGSEMirrorTest extends TestSuper {
	
	protected TroilkattHDFS tfs;
	protected GeoGSEMirror source;

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
		Pipeline pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs);
		
		String localRootDir = tmpDir;
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"), OsPath.join("meta", pipeline.name));
		String hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 0, "geoGSEMirror"));
		String hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"), "tmp");
		
		source = new GeoGSEMirror("geoGSEMirror", null,
				"test/geoGSEMirror", "bz2", 3, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGeoGSEMirror() throws IOException, TroilkattPropertiesException, StageInitException, PipelineException {		
		assertNotNull(source.ftpServer);		
		assertEquals(GeoGSEMirror.GSEftpDir, source.ftpDir);
		assertNotNull(source.adminEmail);
	}
	
	@Test
	public void testDownloadFile() throws StageException, IOException, TroilkattPropertiesException, StageInitException {
		FTPClient ftp = new FTPClient();		
	    assertTrue(source.connectFTP(ftp));
		String hdfsName = source.downloadFile(ftp, "GSE312", 109);
		assertEquals(OsPath.join(source.hdfsOutputDir, "GSE312_family.soft.109.bz2"), hdfsName);
		assertTrue(tfs.isfile(hdfsName));
		
		String localName = tfs.getFile(hdfsName, outDir, tmpDir, logDir);
		assertNotNull(localName);
		String sha1 = FSUtils.sha1file(localName);
		assertEquals("6e7672eb89b82201e3f4e12adce62e102e57893f", sha1);
		
		ftp.disconnect();
	}
}
