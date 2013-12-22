package edu.princeton.function.troilkatt.source;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import edu.princeton.function.troilkatt.fs.LogTableHbase;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class ListDirTest extends TestSuper{
	protected TroilkattProperties troilkattProperties;			
	protected TroilkattHDFS tfs; 
	protected LogTableHbase lt;
	protected Pipeline pipeline;
	
	protected String localRootDir;
	protected String hdfsStageMetaDir;
	protected String hdfsStageTmpDir;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TestSuper.initTestDir();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {		
		troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));		
		FileSystem hdfs = FileSystem.get(new Configuration());			
		tfs = new TroilkattHDFS(hdfs);
		lt = new LogTableHbase("unitPipeline", HBaseConfiguration.create());
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs, lt);
		
		localRootDir = tmpDir;
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), OsPath.join("meta", pipeline.name));
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 0, "listDir"));
		hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "tmp");
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testListDir() throws TroilkattPropertiesException, StageInitException {
		String dirToList = OsPath.join(hdfsRoot, "ts");
		ListDir source = new ListDir("listDir", dirToList,
				"test/source", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);		
		assertEquals(dirToList, source.listDir);
				
		source = new ListDir("listDir", "hdfs://" + dirToList,
				"test/source", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);		
		assertEquals(dirToList, source.listDir);
	}

	// Invalid directory to list
	@Test(expected=StageInitException.class)
	public void testListDir2() throws TroilkattPropertiesException, StageInitException {		
		ListDir source = new ListDir("listDir", "/invalid/directory/to/list",
				"test/source", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);				
		assertNotNull(source);
	}
	
	@Test
	public void testRetrieve() throws TroilkattPropertiesException, StageInitException, StageException {
		String dirToList = OsPath.join(hdfsRoot, "ts");
		ListDir source = new ListDir("listDir", dirToList,
				"test/source", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);				
		
		ArrayList<String> metaFiles = new ArrayList<String>();
		ArrayList<String> logFiles = new ArrayList<String>();
		ArrayList<String> files = source.retrieve(metaFiles, logFiles, 21);
		
		assertEquals(6, files.size());
		Collections.sort(files);
		assertEquals("file1", tfs.getFilenameName(files.get(0)));
		assertEquals("file2", tfs.getFilenameName(files.get(1)));
		assertEquals("file3", tfs.getFilenameName(files.get(2)));
		assertEquals("file4", tfs.getFilenameName(files.get(3)));
		assertEquals("file5", tfs.getFilenameName(files.get(4)));
		assertTrue(files.get(3).contains("subdir1/file4"));
		assertTrue(files.get(4).contains("subdir1/file5"));
		assertEquals("file6", tfs.getFilenameName(files.get(5)));
		assertTrue(files.get(5).contains("subdir2/file6"));
		
		assertEquals(0, metaFiles.size());
		assertEquals(0, logFiles.size());
	}

	
	@Test
	public void testRetrieve2() throws TroilkattPropertiesException, StageInitException, StageException {
		String dirToList = OsPath.join(hdfsRoot, "ts");
		ListDir source = new ListDir("listDir", dirToList,
				"test/source", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);		
		
		ArrayList<String> files = source.retrieve2(22);
		assertEquals(6, files.size());
		Collections.sort(files);
		assertEquals("file1", tfs.getFilenameName(files.get(0)));
		assertEquals("file2", tfs.getFilenameName(files.get(1)));
		assertEquals("file3", tfs.getFilenameName(files.get(2)));
		assertEquals("file4", tfs.getFilenameName(files.get(3)));
		assertEquals("file5", tfs.getFilenameName(files.get(4)));
		assertTrue(files.get(3).contains("subdir1/file4"));
		assertTrue(files.get(4).contains("subdir1/file5"));
		assertEquals("file6", tfs.getFilenameName(files.get(5)));
		assertTrue(files.get(5).contains("subdir2/file6"));		
	}
}
