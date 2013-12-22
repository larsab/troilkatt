package edu.princeton.function.troilkatt.source;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

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
import edu.princeton.function.troilkatt.fs.LogTableHbase;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class ListDirDiffTest extends TestSuper {
	protected TroilkattProperties troilkattProperties;			
	protected TroilkattHDFS tfs; 	
	protected LogTableHbase lt;
	protected Pipeline pipeline;
	protected String srcDir;
	protected String dstDir;
	
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
		
		srcDir = OsPath.join(hdfsRoot, "ls");
		dstDir = OsPath.join(hdfsRoot, "ls2");
		tfs.deleteDir(dstDir);
		tfs.mkdir(dstDir);
		
		localRootDir = tmpDir;
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), OsPath.join("meta", pipeline.name));
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 0, "listDirDiff"));
		hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "tmp");
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testListDirDiff() throws TroilkattPropertiesException, StageInitException {		
		ListDirDiff source = new ListDirDiff("listDirDiff", srcDir + " " + dstDir,
				"test/source", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);		
		assertEquals(srcDir, source.srcDir);
		assertEquals(dstDir, source.dstDir);
	}
	
	// Invalid Dir 1
	@Test(expected=StageInitException.class)
	public void testListDirDiffI1() throws TroilkattPropertiesException, StageInitException {		
		ListDirDiff source = new ListDirDiff("listDirDiff", "/invalid/dir1" + " " + dstDir,
				"test/source", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);		
		assertEquals(srcDir, source.srcDir);
		assertEquals(dstDir, source.dstDir);
	}
	
	// Invalid Dir 3
	@Test(expected=StageInitException.class)
	public void testListDirDiffI2() throws TroilkattPropertiesException, StageInitException {		
		ListDirDiff source = new ListDirDiff("listDirDiff", "/invalid/dir3" + " " + dstDir,
				"test/source", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);		
		assertEquals(srcDir, source.srcDir);
		assertEquals(dstDir, source.dstDir);
	}

	@Test
	public void testRetrieve() throws IOException, TroilkattPropertiesException, StageInitException, StageException {
		ListDirDiff source = new ListDirDiff("listDirDiff", srcDir + " " + dstDir,
				"test/source", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);	
		
		ArrayList<String> metaFiles = new ArrayList<String>();		
		ArrayList<String> logFiles = new ArrayList<String>();
		ArrayList<String> files = source.retrieve(metaFiles, logFiles, 21);		
		assertEquals(6, files.size());
		Collections.sort(files);
		assertTrue(files.get(0).endsWith("file1"));
		assertTrue(files.get(1).endsWith("file2"));
		assertTrue(files.get(2).endsWith("file3"));
		assertTrue(files.get(3).endsWith("subdir1/file4"));
		assertTrue(files.get(4).endsWith("subdir1/file5"));
		assertTrue(files.get(5).endsWith("subdir2/file6"));		
		assertEquals(0, metaFiles.size());
		assertEquals(0, logFiles.size());
		
		Path srcFile = new Path(OsPath.join(dataDir, configurationFile));		
		FileSystem fs = tfs.hdfs;
		fs.mkdirs(new Path(dstDir));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(dstDir, "file1.10000.gz")));
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(dstDir, "file3.11001.bz2")));
		fs.mkdirs(new Path(OsPath.join(dstDir, "subdir1")));		
		fs.copyFromLocalFile(srcFile, new Path(OsPath.join(dstDir, "subdir1/file5.111111.none")));
		
		files = source.retrieve(metaFiles, logFiles, 22);		
		assertEquals(3, files.size());
		Collections.sort(files);		
		assertTrue(files.get(0).endsWith("file2"));		
		assertTrue(files.get(1).endsWith("subdir1/file4"));		
		assertTrue(files.get(2).endsWith("subdir2/file6"));		
		assertEquals(0, metaFiles.size());
		assertEquals(0, logFiles.size());
		
		dstDir = srcDir;
		source = new ListDirDiff("listDirDiff", srcDir + " " + dstDir,
				"test/source", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		files = source.retrieve(metaFiles, logFiles, 22);		
		assertEquals(0, files.size());			
		assertEquals(0, metaFiles.size());
		assertEquals(0, logFiles.size());
	}

}
