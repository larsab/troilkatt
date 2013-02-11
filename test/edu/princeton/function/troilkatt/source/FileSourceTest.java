package edu.princeton.function.troilkatt.source;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class FileSourceTest extends TestSuper {
	protected static TroilkattProperties troilkattProperties;				
	protected static TroilkattHDFS tfs;	
	protected static Pipeline pipeline;
	
	protected static String localRootDir;
	protected static String hdfsStageMetaDir;
	protected static String hdfsStageTmpDir;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		FileSystem hdfs = FileSystem.get(new Configuration());
		tfs = new TroilkattHDFS(hdfs);		
		troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs);
		
		localRootDir = tmpDir;
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), OsPath.join("meta", pipeline.name));
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 0, "executeSource"));
		hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "tmp");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {		
		
		
		OsPath.deleteAll(tmpDir);
		OsPath.mkdir(tmpDir);
	}

	@After
	public void tearDown() throws Exception {
		OsPath.deleteAll(tmpDir);
	}
	
	@Test(expected=StageInitException.class)
	public void testFileSource() throws TroilkattPropertiesException, StageInitException {
		FileSource source = new FileSource("fileSource", "/foo/invalid-file",
				"test/fileSource", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline); 
		assertNull(source);
	}

	@Test
	public void testRetrieve() throws IOException, TroilkattPropertiesException, StageInitException, StageException {
		String inputFile = OsPath.join(tmpDir, "inputFile");
		String[] fileNames = {"troilkatt/data/test/foo.100.gz",
				"troilkatt/data/test/bar.101.gz", 
				"troilkatt/data/test/baz.101.gz"};
		FSUtils.writeTextFile(inputFile, fileNames);
		
		FileSource source = new FileSource("fileSource", inputFile,
				null, null, 0, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		ArrayList<String> metaFiles = new ArrayList<String>();
		ArrayList<String> logFiles = new ArrayList<String>();
		ArrayList<String> hdfsFiles = source.retrieve(metaFiles, logFiles, 107);
		
		assertEquals(3, hdfsFiles.size());
		Collections.sort(hdfsFiles);
		assertEquals("troilkatt/data/test/bar.101.gz", hdfsFiles.get(0));
		assertEquals("troilkatt/data/test/baz.101.gz", hdfsFiles.get(1));
		assertEquals("troilkatt/data/test/foo.100.gz", hdfsFiles.get(2));
		assertEquals(0, metaFiles.size());
		assertEquals(1, logFiles.size());
		assertTrue(fileCmp(inputFile, logFiles.get(0)));
	}
}
