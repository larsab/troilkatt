package edu.princeton.function.troilkatt.fs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.log4j.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.princeton.function.troilkatt.TestSuper;

public class OsPathTest extends TestSuper {	
	public static Logger testLogger;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		testLogger = Logger.getLogger("test");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		initTestDir();
		
		OsPath.deleteAll(tmpDir);
		OsPath.mkdir(tmpDir);
	}

	@After
	public void tearDown() throws Exception {
		OsPath.deleteAll(tmpDir);
	}

	@Test
	public void testJoin() {
		assertEquals("hdfs:///user/larsab/troilkatt/test/foo",
				OsPath.join("hdfs:///user/larsab/troilkatt/test/", "foo"));
		assertEquals("/user/larsab/troilkatt/test/foo",
				OsPath.join("/user/larsab/troilkatt/test/", "foo"));
		assertEquals("troilkatt/test/foo",
				OsPath.join("troilkatt/test/", "foo"));
		assertEquals("troilkatt/test/foo",
				OsPath.join("troilkatt/test", "foo"));
		assertEquals("/user/larsab/troilkatt/test/foo",
				OsPath.join("/user/larsab/troilkatt\\test/", "foo"));
	}
	
	@Test(expected=RuntimeException.class)
	public void testJoin2() {
		assertEquals("foo/bar/baz", OsPath.join("foo/bar", null));
	}

	@Test(expected=RuntimeException.class)
	public void testJoin3() {
		assertEquals("foo/bar/baz", OsPath.join(null, "baz"));
	}

	@Test
	public void testBasename() {
		assertEquals("foo",
				OsPath.basename("troilkatt/test/foo"));
		//assertEquals("foo",
		//		OsPath.basename("C:\\troilkatt\\test\\foo"));
		assertEquals("foo",
				OsPath.basename("troilkatt/test/foo/"));
	}
	
	@Test(expected=RuntimeException.class)
	public void testBasename2() {
		assertEquals("foo",
				OsPath.basename(null));
	}

	@Test
	public void testDirname() {
		assertEquals("troilkatt/test",
				OsPath.dirname("troilkatt/test/foo"));
		//assertEquals("troilkatt/test",
		//		OsPath.basename("C:\\troilkatt\\test\\foo"));
		assertEquals("troilkatt/test",
				OsPath.dirname("troilkatt/test/foo/"));		
	}
	
	@Test(expected=RuntimeException.class)
	public void testDirname2() {
		assertEquals("troilkatt/test",
				OsPath.dirname(null));
	}
	
	@Test
	public void testGetLastExtension() {
		assertEquals("gz", OsPath.getLastExtension("foo.bar.gz"));
		assertEquals("bar", OsPath.getLastExtension("foo.bar"));
		assertEquals("foo", OsPath.getLastExtension("foo"));
		assertEquals("foo", OsPath.getLastExtension(".foo"));
		assertNull(OsPath.getLastExtension(""));
		assertNull(OsPath.getLastExtension(null));
	}
	
	@Test
	public void testReplaceLastExtension() {
		assertEquals("foo.bar.bz", OsPath.replaceLastExtension("foo.bar.gz", "bz"));
		assertEquals("foo.bz", OsPath.replaceLastExtension("foo.bar", "bz"));
		assertEquals("foo.bz", OsPath.replaceLastExtension("foo", "bz"));
		assertEquals(".bz", OsPath.replaceLastExtension(".foo", "bz"));
		assertNull(OsPath.replaceLastExtension(null, "bz"));
		assertNull(OsPath.replaceLastExtension("", "bz"));
		assertNull(OsPath.replaceLastExtension("foo.bar", null));
	}
	
	@Test
	public void testIsfile() {
		assertTrue(OsPath.isfile(OsPath.join(dataDir, "files/file1")));
		assertFalse(OsPath.isfile(OsPath.join(dataDir, "files/file-non-existing")));
		assertFalse(OsPath.isfile(dataDir));
	}
	
	@Test(expected=RuntimeException.class)
	public void testIsfile2() {
		assertTrue(OsPath.isfile(null));
	}

	@Test
	public void testIsdir() {
		assertFalse(OsPath.isdir(OsPath.join(dataDir, "files/file1")));
		assertFalse(OsPath.isdir(OsPath.join(dataDir, "files/file-non-existing")));
		assertTrue(OsPath.isdir(dataDir));
	}
	
	@Test(expected=RuntimeException.class)
	public void testIsdir2() {
		assertFalse(OsPath.isdir(null));
	}
	
	@Test
	public void testFileSize() throws IOException {
		assertEquals(8396983, OsPath.fileSize(OsPath.join(dataDir, "files/file1")));
		
		String srcFile = OsPath.join(tmpDir, "empty");
		byte[] empty = new byte[0];
		FSUtils.writeFile(srcFile, empty);
		assertEquals(0, OsPath.fileSize(srcFile));
	}

	@Test
	public void testCopy() throws IOException {
		assertTrue(OsPath.copy(OsPath.join(dataDir, "files/file1"),
				OsPath.join(tmpDir, "file1")));
		assertTrue(fileCmp(OsPath.join(dataDir, "files/file1"),
				OsPath.join(tmpDir, "file1")));
		
		
		assertFalse(OsPath.copy(OsPath.join(dataDir, "files/non-existnig"),
				OsPath.join(tmpDir, "file1")));
		assertFalse(OsPath.copy(dataDir,
				OsPath.join(tmpDir, "file1")));
		assertFalse(OsPath.copy(OsPath.join(dataDir, "files/file1"),
				OsPath.join("/foo/bar", "file1")));
		assertFalse(OsPath.copy(OsPath.join(dataDir, "files/file1"),
				OsPath.join(tmpDir, "")));
	}
	
	@Test(expected=RuntimeException.class)
	public void testCopy2() throws IOException {
		assertTrue(OsPath.copy(null, OsPath.join(tmpDir, "file1")));
	}
	
	@Test(expected=RuntimeException.class)
	public void testCopy3() throws IOException {
		assertTrue(OsPath.copy(OsPath.join(dataDir, "files/file1"),	null));
	}

	@Test
	public void testRename() throws IOException {
		assertTrue(OsPath.copy(OsPath.join(dataDir, "files/file1"),
				OsPath.join(tmpDir, "file1")));
		assertTrue(OsPath.rename(OsPath.join(tmpDir, "file1"), 
				OsPath.join(tmpDir, "file2")));
		assertTrue(fileCmp(OsPath.join(dataDir, "files/file1"),
				OsPath.join(tmpDir, "file2")));
		
		assertFalse(OsPath.rename(OsPath.join(tmpDir, "file1"), 
				OsPath.join(tmpDir, "file2")));
		assertFalse(OsPath.rename(OsPath.join(tmpDir, "file2"), 
				OsPath.join("/foo/bar", "file2")));
		
	}

	@Test(expected=RuntimeException.class)
	public void testRename2() {		
		assertTrue(OsPath.rename(null, OsPath.join(tmpDir, "file2")));
	}
	
	@Test(expected=RuntimeException.class)
	public void testRename3() {		
		assertTrue(OsPath.rename(OsPath.join(tmpDir, "file1"), null)); 				
	}
	
	@Test
	public void testDelete() throws IOException {
		String dstFile = OsPath.join(tmpDir, "file1");
		assertTrue(OsPath.copy(OsPath.join(dataDir, "files/file1"),	dstFile));
		assertTrue(OsPath.isfile(dstFile));
		assertTrue(OsPath.delete(dstFile));
		assertFalse(OsPath.isfile(dstFile));
		assertFalse(OsPath.delete(dstFile));
	}
	
	@Test(expected=RuntimeException.class)
	public void testDelete2() {
		assertFalse(OsPath.delete(null));
	}
	
	@Test
	public void testDeleteAll() throws IOException {
		String dstFile1 = OsPath.join(tmpDir, "file1");
		String dstFile2 = OsPath.join(tmpDir, "file2");
		String dstFile3 = OsPath.join(tmpDir, "subdir/file3");
		assertTrue(OsPath.copy(OsPath.join(dataDir, "files/file1"),	dstFile1));
		assertTrue(OsPath.copy(OsPath.join(dataDir, "files/file1"),	dstFile2));
		assertTrue(OsPath.mkdir(OsPath.join(tmpDir, "subdir")));
		assertTrue(OsPath.copy(OsPath.join(dataDir, "files/file1"),	dstFile3));
		assertTrue(OsPath.isfile(dstFile1));
		assertTrue(OsPath.isfile(dstFile2));
		assertTrue(OsPath.isfile(dstFile3));
		assertTrue(OsPath.deleteAll(tmpDir));
		assertFalse(OsPath.isfile(dstFile1));
		assertFalse(OsPath.isfile(dstFile2));
		assertFalse(OsPath.isfile(dstFile3));
		
		assertFalse(OsPath.deleteAll("/foo/bar"));		
		assertFalse(OsPath.deleteAll(dstFile1));
	}

	@Test(expected=RuntimeException.class)
	public void testDeleteAll2() {
		assertFalse(OsPath.deleteAll(null));
	}
	
	@Test
	public void testMkdir() {
		String testDir = OsPath.join(tmpDir, "foo/bar/baz/");
		if (OsPath.isdir(OsPath.join(tmpDir, "foo"))) {
			assertTrue(OsPath.rmdirR(OsPath.join(tmpDir, "foo")));
		}
		
		assertTrue(OsPath.mkdir(testDir));
		assertTrue(OsPath.mkdir(testDir));	
		
		assertFalse(OsPath.mkdir("/foo/bar"));
	}
	
	@Test(expected=RuntimeException.class)
	public void testMkdir2() {
		assertTrue(OsPath.mkdir(null));
	}
	
	@Test
	public void testRmdir() {
		assertTrue(OsPath.mkdir(OsPath.join(tmpDir, "foo/bar/baz/")));
		
		assertTrue(OsPath.rmdir(OsPath.join(tmpDir, "foo/bar/baz")));
		assertTrue(OsPath.rmdirR(OsPath.join(tmpDir, "foo")));
		assertFalse(OsPath.rmdirR(OsPath.join(tmpDir, "foo/bar")));
	}
	
	@Test(expected=RuntimeException.class)
	public void testRmdir2() {
		assertTrue(OsPath.rmdir(null));
	}
	
	@Test
	public void testRmdirR() {
		assertTrue(OsPath.mkdir(OsPath.join(tmpDir, "foo/bar/baz/")));
		
		assertTrue(OsPath.rmdirR(OsPath.join(tmpDir, "foo/bar/")));
		assertTrue(OsPath.rmdirR(OsPath.join(tmpDir, "foo")));
		assertFalse(OsPath.rmdirR(OsPath.join(tmpDir, "foo")));
	}
	
	@Test(expected=RuntimeException.class)
	public void testRmdirR2() {
		assertTrue(OsPath.rmdir(null));
	}
	
	// Just a wrapper function
	@Test
	public void testNormPath() {
		assertEquals("/foo/baz/bongo", OsPath.normPath("/foo//bar/../baz/./bongo"));
	}

	@Test
	public void testGetRelativePath() {
		assertEquals("baz", OsPath.getRelativePath("/foo/bar", "/foo/bar/baz"));
		assertEquals("bar/baz", OsPath.getRelativePath("/foo", "/foo/bar/baz"));
		assertEquals("baz", OsPath.getRelativePath("bar", "/foo/bar/baz"));
		assertNull(OsPath.getRelativePath("/foo/bongo", "/foo/bar/baz"));
	}
	
	@Test
	public void testRelative2absoluteStringStringArray() {
		String[] files = {"bar", "baz"};
		String[] absoluteNames = OsPath.relative2absolute("foo", files);
		assertEquals(2, absoluteNames.length);
		assertEquals("foo/bar", absoluteNames[0]);
		assertEquals("foo/baz", absoluteNames[1]);
	}
	
	@Test
	public void testRelative2absoluteArrays() {
		ArrayList<String> files = new ArrayList<String>();
		files.add("bar");
		files.add("baz");
		ArrayList<String> absoluteNames = OsPath.relative2absolute("foo", files);
		assertEquals(2, absoluteNames.size());
		assertEquals("foo/bar", absoluteNames.get(0));
		assertEquals("foo/baz", absoluteNames.get(1));
	}

	@Test
	public void testRelative2absoluteStringString() {
		assertEquals("foo/bar", OsPath.relative2absolute("foo", "bar"));
	}

	@Test
	public void testListdir() throws IOException {
		initTestDir();
		
		String[] files = OsPath.listdir(OsPath.join(dataDir, "ls"), testLogger);
		
		assertNotNull(files);
		assertEquals(6, files.length);
		Arrays.sort(files);
		assertTrue(files[0].endsWith("file1"));
		assertTrue(files[1].endsWith("file2"));
		assertTrue(files[2].endsWith("file3"));
		
		files = OsPath.listdir(OsPath.join(dataDir, "ls"));
		
		assertNotNull(files);
		assertEquals(6, files.length);
		Arrays.sort(files);
		assertTrue(files[0].endsWith("file1"));
		assertTrue(files[1].endsWith("file2"));
		assertTrue(files[2].endsWith("file3"));					
	}
	
	// Empty directory
	@Test
	public void testListdir2() throws IOException {		
		String[] files = OsPath.listdir(OsPath.join(dataDir, "ls/subdir3"), testLogger);
		assertNotNull(files);
		assertEquals(0, files.length);
		
		files = OsPath.listdir(OsPath.join(dataDir, "ls/subdir3"));
		assertNotNull(files);
		assertEquals(0, files.length);	
	}
	
	// Invalid directory
	@Test
	public void testListdir3() throws IOException {			
		String[] files = OsPath.listdir(OsPath.join(dataDir, "ls/invalid-subdir"), testLogger);		
		assertNull(files);		
		
		files = OsPath.listdir(OsPath.join(dataDir, "ls/invalid-subdir"));		
		assertNull(files);
	}

	@Test
	public void testListdirR() throws IOException {
		initTestDir();
		
		String[] files = OsPath.listdirR(OsPath.join(dataDir, "ls"), testLogger);
		
		assertNotNull(files);
		assertEquals(6, files.length);
		Arrays.sort(files);
		assertTrue(files[0].endsWith("file1"));
		assertTrue(files[1].endsWith("file2"));
		assertTrue(files[2].endsWith("file3"));
		assertTrue(files[3].endsWith("subdir1/file4"));
		assertTrue(files[4].endsWith("subdir1/file5"));
		assertTrue(files[5].endsWith("subdir2/file6"));
		
		files = OsPath.listdirR(OsPath.join(dataDir, "ls"));
		
		assertNotNull(files);
		assertEquals(6, files.length);
		Arrays.sort(files);
		assertTrue(files[0].endsWith("file1"));
		assertTrue(files[1].endsWith("file2"));
		assertTrue(files[2].endsWith("file3"));
		assertTrue(files[3].endsWith("subdir1/file4"));
		assertTrue(files[4].endsWith("subdir1/file5"));
		assertTrue(files[5].endsWith("subdir2/file6"));
	}
	
	// Empty directory
	@Test
	public void testListdirR2() throws IOException {		
		String[] files = OsPath.listdirR(OsPath.join(dataDir, "ls/subdir3"), testLogger);
		assertNotNull(files);
		assertEquals(0, files.length);		
		
		files = OsPath.listdirR(OsPath.join(dataDir, "ls/subdir3"));
		assertNotNull(files);
		assertEquals(0, files.length);
	}
	
	// Invalid directory
	@Test
	public void testListdirR3() throws IOException {			
		String[] files = OsPath.listdirR(OsPath.join(dataDir, "ls/invalid-subdir"), testLogger);
		assertNull(files);		
		
		files = OsPath.listdirR(OsPath.join(dataDir, "ls/invalid-subdir"));
		assertNull(files);
	}	
	
	@Test
	public void testFileInList() {
		ArrayList<String> list = new ArrayList<String>();
		list.add("/parent1/sub1/sub2/foo");
		list.add("/parent1/sub1/sub2/bar");
		list.add("/parent1/sub1/sub2/baz");
		
		assertTrue(OsPath.fileInList(list, "bar", false));
		assertTrue(OsPath.fileInList(list, "bar", true));
		assertTrue(OsPath.fileInList(list, "ba", true));
		assertFalse(OsPath.fileInList(list, "ba", false));
		assertFalse(OsPath.fileInList(list, "sub1", false));
		assertFalse(OsPath.fileInList(list, "sub1", true));
		
		String[] array = {"/parent1/sub1/sub2/foo", "/parent1/sub1/sub2/bar", "/parent1/sub1/sub2/baz"};
		
		assertTrue(OsPath.fileInList(array, "bar", false));
		assertTrue(OsPath.fileInList(array, "bar", true));
		assertTrue(OsPath.fileInList(array, "ba", true));
		assertFalse(OsPath.fileInList(array, "ba", false));
		assertFalse(OsPath.fileInList(array, "sub1", false));
		assertFalse(OsPath.fileInList(array, "sub1", true));
	}
}
