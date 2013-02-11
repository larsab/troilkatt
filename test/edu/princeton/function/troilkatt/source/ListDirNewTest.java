package edu.princeton.function.troilkatt.source;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

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

public class ListDirNewTest extends TestSuper {
	protected TroilkattProperties troilkattProperties = null;			
	protected TroilkattHDFS tfs = null;
	protected Pipeline pipeline;
	
	protected String localRootDir;
	protected String hdfsStageMetaDir;
	protected String hdfsStageTmpDir;
	
	protected String dirToList;
	protected ListDirNew source;

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
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs);
		
		localRootDir = tmpDir;
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), OsPath.join("meta", pipeline.name));
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 0, "listDirNew"));
		hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "tmp");
		
		dirToList = OsPath.join(hdfsRoot, "ts");
		source = new ListDirNew("listDirNew", dirToList,
				"test/source", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);		
	}

	@After
	public void tearDown() throws Exception {
	}
	
	//@Test
	public void testListDirNew() throws TroilkattPropertiesException, StageInitException {
		assertEquals(dirToList, source.listDir);
	}
	
	@Test
	public void testReadMetaFile() throws IOException, StageException {				
		String metaFile = OsPath.join(source.stageMetaDir, "filelist");	
		String[] metaLines = {
				OsPath.join(dirToList, "file1"), 
				OsPath.join(dirToList, "file2"), 
				OsPath.join(dirToList, "subdir1/file4")};		
		
		FSUtils.writeTextFile(metaFile, metaLines);		
		ArrayList<String> metaFiles = new ArrayList<String>();
		metaFiles.add(metaFile);
		
		HashSet<String> prevFiles = source.readMetaFile(metaFiles, "filelist", dirToList);
		assertEquals(3, prevFiles.size());
		assertTrue(prevFiles.contains("file1"));
		assertTrue(prevFiles.contains("file2"));
		assertTrue(prevFiles.contains("subdir1/file4"));
		
		metaLines = new String[0];
		FSUtils.writeTextFile(metaFile, metaLines);		
		metaFiles = new ArrayList<String>();
		metaFiles.add(metaFile);
		
		prevFiles = source.readMetaFile(metaFiles, "filelist", dirToList);
		assertEquals(0, prevFiles.size());
	}
	
	// Invalid metafile
	@Test(expected=StageException.class)
	public void testReadMetaFile2() throws IOException, StageException {				
		String metaFile = OsPath.join(source.stageMetaDir, "invalidNAme");	
		String[] metaLines = {
				OsPath.join(hdfsRoot, "ls/file1"), 
				OsPath.join(hdfsRoot, "ls/file2"), 
				OsPath.join(hdfsRoot, "ls/subdir1/file4")};		
		
		FSUtils.writeTextFile(metaFile, metaLines);		
		ArrayList<String> metaFiles = new ArrayList<String>();
		metaFiles.add(metaFile);
		
		HashSet<String> prevFiles = source.readMetaFile(metaFiles, metaFile, dirToList);
		assertEquals(3, prevFiles.size());
	}
	
	// Invalid filename in metafile
	@Test(expected=StageException.class)
	public void testReadMetaFile3() throws IOException, StageException {				
		String metaFile = OsPath.join(source.stageMetaDir, "invalidNAme");	
		String[] metaLines = {
				OsPath.join(dirToList, "file1"), 
				OsPath.join(dirToList, "file2"), 
		        "/foo/bar/baz"};		

		FSUtils.writeTextFile(metaFile, metaLines);		
		ArrayList<String> metaFiles = new ArrayList<String>();
		metaFiles.add(metaFile);

		HashSet<String> prevFiles = source.readMetaFile(metaFiles, metaFile, dirToList);
		assertEquals(3, prevFiles.size());
	}
	
	@Test
	public void testAbsolute2relative() throws StageException {
		ArrayList<String> afs = new ArrayList<String>();
		afs.add(OsPath.join(dirToList, "file1")); 
		afs.add(OsPath.join(dirToList, "file2")); 
		afs.add(OsPath.join(dirToList, "subdir1/file4"));
		
		ArrayList<String> rfs = ListDirNew.absolute2relative(hdfsRoot, afs);
		assertEquals(3, rfs.size());
		assertEquals("ts/file1", rfs.get(0));
		assertEquals("ts/file2", rfs.get(1));
		assertEquals("ts/subdir1/file4", rfs.get(2));
		
		afs.clear();
		rfs = ListDirNew.absolute2relative(hdfsRoot, afs);
		assertEquals(0, rfs.size());
	}
	
	@Test(expected=StageException.class)
	public void testAbsolute2relative2() throws StageException {
		ArrayList<String> afs = new ArrayList<String>();
		afs.add(OsPath.join(dirToList, "file1")); 
		afs.add(OsPath.join(dirToList, "file2")); 
		afs.add("/foo/bar/baz");
		
		ListDirNew.absolute2relative(dirToList, afs);		
	}
	
	@Test
	public void testCompareSets() {
		HashSet<String> setA = new HashSet<String>();
		ArrayList<String> setB = new ArrayList<String>();
		
		setA.add("a");		
		setA.add("d");
		
		setB.add("a");
		setB.add("b");
		setB.add("c");
		setB.add("d");
		
		ArrayList<String> setC = ListDirNew.compareSets(setA, setB);
		assertEquals(2, setC.size());
		assertEquals("b", setC.get(0));
		assertEquals("c", setC.get(1));
		
		setB.clear();
		setB.add("a");
		setB.add("d");		
		setA.add("a");
		setA.add("d");		
		setC = ListDirNew.compareSets(setA, setB);
		assertEquals(0, setC.size());
		
		setA.clear();
		setC = ListDirNew.compareSets(setA, setB);
		assertEquals(2, setC.size());
		assertEquals("a", setC.get(0));
		assertEquals("d", setC.get(1));
		
		setB.clear();
		setA.add("a");
		setA.add("d");
		setC = ListDirNew.compareSets(setA, setB);
		assertEquals(0, setC.size());
	}
	
	@Test
	public void testRetrieve() throws TroilkattPropertiesException, StageInitException, StageException, IOException {		
		ListDirNew source = new ListDirNew("listDirNew", dirToList,
				"test/source", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);		
		assertEquals(dirToList, source.listDir);
		
		String metaFile = OsPath.join(source.stageMetaDir, "filelist");		
		FSUtils.writeTextFile(metaFile, new String[0]);
		
		ArrayList<String> metaFiles = new ArrayList<String>();
		metaFiles.add(metaFile);
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
		
		assertEquals(1, metaFiles.size());
		metaFile = metaFiles.get(0);
		assertEquals(0, logFiles.size());
		
		String[] metaFileLines = FSUtils.readTextFile(metaFile);
		assertEquals(6, metaFileLines.length);
		Arrays.sort(metaFileLines);
		
		String[] newLines = new String[3];
		newLines[0] = metaFileLines[0];
		newLines[1] = metaFileLines[2];
		newLines[2] = metaFileLines[5];
		FSUtils.writeTextFile(metaFile, newLines);
		
		files = source.retrieve(metaFiles, logFiles, 22);
		
		assertEquals(3, files.size());
		Collections.sort(files);
		
		assertEquals("file2", tfs.getFilenameName(files.get(0)));
		assertEquals("file4", tfs.getFilenameName(files.get(1)));
		assertEquals("file5", tfs.getFilenameName(files.get(2)));
		assertTrue(files.get(1).contains("subdir1/file4"));
		assertTrue(files.get(2).contains("subdir1/file5"));
		
		assertEquals(1, metaFiles.size());
		assertEquals(0, logFiles.size());
		
		files = source.retrieve(metaFiles, logFiles, 23);
		assertEquals(0, files.size());
		assertEquals(1, metaFiles.size());
		assertEquals(0, logFiles.size());
	}

}
