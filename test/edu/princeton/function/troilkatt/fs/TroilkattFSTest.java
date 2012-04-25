package edu.princeton.function.troilkatt.fs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.princeton.function.troilkatt.TestSuper;
import edu.princeton.function.troilkatt.Troilkatt;
import edu.princeton.function.troilkatt.TroilkattProperties;

public class TroilkattFSTest extends TestSuper {
	public Configuration hdfsConfig;
	public FileSystem hdfs;
	public TroilkattFS tfs;
	public TroilkattProperties troilkattProperties;	
	public ArrayList<String> localFiles;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TestSuper.initTestDir();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {		
		hdfs = FileSystem.get(new Configuration());
		troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));				
		tfs = new TroilkattFS(hdfs);
		
		OsPath.deleteAll(tmpDir);
		OsPath.mkdir(tmpDir);
		OsPath.deleteAll(logDir);
		OsPath.mkdir(logDir);
		OsPath.deleteAll(outDir);
		OsPath.mkdir(outDir);
		hdfs.delete(new Path(OsPath.join(hdfsRoot, "out")), true);
		
		localFiles = new ArrayList<String>();
		localFiles.add(OsPath.join(dataDir, "files/file1"));
		localFiles.add(OsPath.join(dataDir, "files/file2"));
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testTroilkattFS() {
		TroilkattFS tfs = new TroilkattFS(hdfs);
		assertEquals(tfs.hdfs, hdfs);
		assertNotNull(tfs.logger);
	}

	
	@Test
	public void testListdir() throws IOException {		
		ArrayList<String> files = tfs.listdir(OsPath.join(hdfsRoot, "ls"));
		
		assertEquals(6, files.size());
		Collections.sort(files);
		assertTrue(files.get(0).endsWith("file1"));
		assertTrue(files.get(1).endsWith("file2"));
		assertTrue(files.get(2).endsWith("file3"));		
	}
	
	// Empty directory
	@Test
	public void testListdir2() throws IOException {		
		ArrayList<String> files = tfs.listdir(OsPath.join(hdfsRoot, "ls/subdir3"));
		assertEquals(0, files.size());		
	}
	
	// Invalid directory
	@Test
	public void testListdir3() throws IOException {			
		ArrayList<String> files = tfs.listdir(OsPath.join(hdfsRoot, "ls/invalid-subdir"));
		assertNull(files);		
	}

	@Test
	public void testCompressUncompressFile() throws IOException {
		String srcName = OsPath.join(dataDir, "files/file1");
		String uncompressedName = OsPath.join(outDir, "file1");
		
		String compressedName = tfs.compressFile(srcName, tmpDir, logDir, "gz");
		assertNotNull(compressedName);
		assertTrue( tfs.uncompressFile(compressedName, uncompressedName, logDir) );
		assertTrue(fileCmp(uncompressedName, srcName));
		
		OsPath.deleteAll(outDir);
		OsPath.mkdir(outDir);
		compressedName = tfs.compressFile(srcName, tmpDir, logDir, "bz2");
		assertNotNull(compressedName);
		assertTrue( tfs.uncompressFile(compressedName, uncompressedName, logDir) );
		assertTrue(fileCmp(uncompressedName, srcName));
		
		// Zip compression is not supported
		OsPath.deleteAll(outDir);
		OsPath.mkdir(outDir);
		compressedName = tfs.compressFile(srcName, tmpDir, logDir, "zip");
		assertNull(compressedName);
		//assertTrue( tfs.uncompressFile(compressedName, uncompressedName, logDir) );
		//assertTrue(fileCmp(uncompressedName, srcName));
	}

	// Invalid filename
	@Test
	public void testCompressFile2() throws IOException {
		String compressedName = tfs.compressFile(OsPath.join(dataDir, "files/non-existing"), tmpDir, logDir, "gz");
		assertNull(compressedName);
	}
	
	// Invalid tmpDir
	@Test
	public void testCompressFile3() throws IOException {
		String compressedName = tfs.compressFile(OsPath.join(dataDir, "files/file1"), "/foo", logDir, "gz");
		assertNull(compressedName);
	}
	
	// Invalid logDir
	@Test
	public void testCompressFile4() throws IOException {
		String compressedName = tfs.compressFile(OsPath.join(dataDir, "files/file1"), tmpDir, "/foo", "gz");
		assertNull(compressedName);
	}
	
	// Invalid compression
	@Test
	public void testCompressFile5() throws IOException {
		String compressedName = tfs.compressFile(OsPath.join(dataDir, "files/file1"), tmpDir, logDir, "foo");
		assertNull(compressedName);
	}
	
	// Invalid filename
	@Test
	public void testUncompressFile2() {
		assertFalse( tfs.uncompressFile(OsPath.join(dataDir, "files/non-existing.gz"), 
				OsPath.join(tmpDir, "non-existing"), logDir) );
	}
	
	// Invalid file
	//@Test
	//public void testUncompressFile3() {
	//	assertFalse( tfs.uncompressFile(OsPath.join(dataDir, "files/invalid.5.gz"), 
	//			OsPath.join(tmpDir, "invalid"), logDir) );
	//}
	
	// Invalid compression format
	@Test
	public void testUncompressFile4() {
		assertFalse( tfs.uncompressFile(OsPath.join(dataDir, "files/invalid6.boink"), 
				OsPath.join(tmpDir, "invalid"), logDir) );
	}
	
	// Invalid uncompressed name
	@Test
	public void testUncompressFile5() {
		assertFalse( tfs.uncompressFile(OsPath.join(dataDir, "files/file1.1.gz"), outDir, logDir) );
	}
	
	// Invalid log dir
	@Test
	public void testUncompressFile6() {
		assertFalse( tfs.uncompressFile(OsPath.join(dataDir, "files/file1.1.gz"), 
				OsPath.join(tmpDir, "file1"), "/foo") );
	}
	
	@Test
	public void testCompressUncompressDirectory() throws IOException {	
		OsPath.delete(outDir);
		OsPath.mkdir(outDir);
		OsPath.copy(OsPath.join(dataDir, "files/file1"), OsPath.join(outDir, "file1"));
		OsPath.copy(OsPath.join(dataDir, "files/file2"), OsPath.join(outDir, "file2"));
		String compressedDir = tfs.compressDirectory(outDir, tmpDir, logDir, "tar.gz");
		assertNotNull(compressedDir);
		assertTrue( OsPath.isfile(OsPath.join(tmpDir, "output.tar.gz")) );
		OsPath.delete(outDir);
		OsPath.mkdir(outDir);
		ArrayList<String> files = tfs.uncompressDirectory(compressedDir, outDir, logDir);
		assertEquals(2, files.size());
		Collections.sort(files);		
		assertTrue(fileCmp(files.get(0), OsPath.join(dataDir, "files/file1")));
		assertTrue(fileCmp(files.get(1), OsPath.join(dataDir, "files/file2")));
		
		OsPath.delete(outDir);
		OsPath.mkdir(outDir);
		OsPath.copy(OsPath.join(dataDir, "files/file1"), OsPath.join(outDir, "file1"));
		OsPath.copy(OsPath.join(dataDir, "files/file2"), OsPath.join(outDir, "file2"));
		compressedDir = tfs.compressDirectory(outDir, tmpDir, logDir, "tar.bz2");
		assertNotNull(compressedDir);
		assertTrue( OsPath.isfile(OsPath.join(tmpDir, "output.tar.bz2")) );
		OsPath.delete(outDir);
		OsPath.mkdir(outDir);
		files = tfs.uncompressDirectory(compressedDir, outDir, logDir);
		assertEquals(2, files.size());
		Collections.sort(files);		
		assertTrue(fileCmp(files.get(0), OsPath.join(dataDir, "files/file1")));
		assertTrue(fileCmp(files.get(1), OsPath.join(dataDir, "files/file2")));
		
		OsPath.delete(outDir);
		OsPath.mkdir(outDir);
		OsPath.copy(OsPath.join(dataDir, "files/file1"), OsPath.join(outDir, "file1"));
		OsPath.copy(OsPath.join(dataDir, "files/file2"), OsPath.join(outDir, "file2"));
		compressedDir = tfs.compressDirectory(outDir, tmpDir, logDir, "tar");
		assertNotNull(compressedDir);
		assertTrue( OsPath.isfile(OsPath.join(tmpDir, "output.tar")) );
		OsPath.delete(outDir);
		OsPath.mkdir(outDir);
		files = tfs.uncompressDirectory(compressedDir, outDir, logDir);
		assertEquals(2, files.size());
		Collections.sort(files);		
		assertTrue(fileCmp(files.get(0), OsPath.join(dataDir, "files/file1")));
		assertTrue(fileCmp(files.get(1), OsPath.join(dataDir, "files/file2")));
				
		// Zlib not supported
		compressedDir = tfs.compressDirectory(outDir, tmpDir, logDir, "zip");
		assertNull(compressedDir);
		//assertTrue( OsPath.isfile(OsPath.join(tmpDir, "files.zip")) );
		//OsPath.delete(outDir);
		//OsPath.mkdir(outDir);
		//files = tfs.uncompressDirectory(compressedDir, outDir, logDir);
		//assertEquals(2, files.size());
		//Collections.sort(files);		
		//assertTrue(fileCmp(files.get(0), OsPath.join(dataDir, "files/file1")));
		//assertTrue(fileCmp(files.get(1), OsPath.join(dataDir, "files/file2")));		
	}

	// Invalid source dir
	@Test
	public void testCompressDirectory2() throws IOException {
		String srcDir = OsPath.join(dataDir, "files-non-existing");
		String compressedDir = tfs.compressDirectory(srcDir, tmpDir, logDir, "tar.gz");
		assertNull(compressedDir);
	}
	
	// Invalid out dir
	@Test
	public void testCompressDirectory3() throws IOException {
		String srcDir = OsPath.join(dataDir, "files");
		String compressedDir = tfs.compressDirectory(srcDir, "/foo", logDir, "tar.gz");
		assertNull(compressedDir);
	}
	// Invalid log dir
	@Test
	public void testCompressDirectory4() throws IOException {
		String srcDir = OsPath.join(dataDir, "files");
		String compressedDir = tfs.compressDirectory(srcDir, tmpDir, "/foo", "tar.gz");
		assertNull(compressedDir);
	}
	
	// Invalid compression format
	@Test
	public void testCompressDirectory5() throws IOException {
		String srcDir = OsPath.join(dataDir, "files");
		String compressedDir = tfs.compressDirectory(srcDir, tmpDir, logDir, "dfdz");
		assertNull(compressedDir);
	}
	
	// Non-existing file
	@Test
	public void testUncompressDirectory2() {
		assertNull( tfs.uncompressDirectory("/foo/bar.tar.gz", outDir, logDir) );	
	}
	
	// Invalid file
	@Test
	public void testUncompressDirectory3() {
		assertNull( tfs.uncompressDirectory(OsPath.join(dataDir, "invalid.tar.gz"), outDir, logDir) );
	}
	
	// Invalid compression format
	public void testUncompressDirectory4() {
		assertNull( tfs.uncompressDirectory(OsPath.join(dataDir, "files/file1"), outDir, logDir) );
	}
	
	// Invalid tmp dir
	public void testUncompressDirectory5() {
		assertNull( tfs.uncompressDirectory(OsPath.join(dataDir, "dir.tar.gz"), "/foo", logDir) );
	}
	
	// Invalid log dir
	public void testUncompressDirectory6() {
		assertNull( tfs.uncompressDirectory(OsPath.join(dataDir, "dir.tar.gz"), outDir, "/foo") );
	}
	
	@Test
	public void testIsfile() throws IOException {
		assertTrue( tfs.isfile(OsPath.join(hdfsRoot, "ls/file1")) );
		assertFalse( tfs.isfile(OsPath.join(hdfsRoot, "ls")) );
		assertFalse( tfs.isfile(OsPath.join(hdfsRoot, "ls/non-existing")) );		
	}
	
	@Test
	public void testIsdir() throws IOException {
		assertFalse( tfs.isdir(OsPath.join(hdfsRoot, "ls/file1")) );
		assertTrue( tfs.isdir(OsPath.join(hdfsRoot, "ls")) );
		assertFalse( tfs.isdir(OsPath.join(hdfsRoot, "ls/non-existing")) );
	}

	@Test
	public void testMkdir() throws IOException {
		String hout = OsPath.join(hdfsRoot, "out/test");
		tfs.mkdir(hout);
		assertTrue( tfs.isdir(hout) );
	}
	
	// Invalid dir
	// Driectory persmissions not respected in pseudo mode
	//@Test(expected=IOException.class)
	//public void testMkdir2() throws IOException {
	//	String hout = "/foo/bar";
	//	tfs.mkdir(hout);		
	//}

	@Test
	public void testDelete() throws IOException {
		String dstFile = OsPath.join(hdfsRoot, "delete/file1");		
		hdfs.copyFromLocalFile(false, true, 
				new Path(OsPath.join(dataDir, "files/file1")), 
				new Path(dstFile));
		assertTrue(tfs.isfile(dstFile));
		assertTrue(tfs.deleteFile(dstFile));
		assertFalse(tfs.isfile(dstFile));
		assertFalse(tfs.deleteFile(dstFile));
	}
	
	@Test(expected=RuntimeException.class)
	public void testDelete2() throws IOException {
		assertFalse(tfs.deleteFile(null));
	}
	
	@Test
	public void testDeleteDir() throws IOException {
		String deleteDir = OsPath.join(hdfsRoot, "delete");
		String dstFile1 = OsPath.join(hdfsRoot, "delete/file1");
		String dstFile2 = OsPath.join(hdfsRoot, "delete/file2");
		String dstFile3 = OsPath.join(hdfsRoot, "delete/subdir/file3");
		hdfs.copyFromLocalFile(false, true,
				new Path(OsPath.join(dataDir, "files/file1")),
				new Path(dstFile1));
		hdfs.copyFromLocalFile(false, true,
				new Path(OsPath.join(dataDir, "files/file2")),
				new Path(dstFile2));
		tfs.mkdir(OsPath.join(hdfsRoot, "delete/subdir"));
		hdfs.copyFromLocalFile(false, true,
				new Path(OsPath.join(dataDir, "files/file1")),
				new Path(dstFile3));		
		assertTrue(tfs.isfile(dstFile1));
		assertTrue(tfs.isfile(dstFile2));
		assertTrue(tfs.isfile(dstFile3));
		assertTrue(tfs.deleteDir(deleteDir));
		assertFalse(tfs.isfile(dstFile1));
		assertFalse(tfs.isfile(dstFile2));
		assertFalse(tfs.isfile(dstFile3));
		
		// Not respected by HDFS in pseudo mode
		//assertFalse(tfs.deleteDir("/foo/bar"));
		
		hdfs.copyFromLocalFile(false, true,
				new Path(OsPath.join(dataDir, "files/file1")),
				new Path(dstFile1));
		assertFalse(tfs.deleteDir(dstFile1));
		assertTrue(tfs.deleteDir(OsPath.join(hdfsRoot, "delete")));
	}

	@Test(expected=RuntimeException.class)
	public void testDeleteAll2() {
		assertFalse(OsPath.deleteAll(null));
	}
	
	@Test
	public void testCleanupDir() throws IOException {
		String cleanupRoot = OsPath.join(hdfsRoot, "cleanup");          // on HDFS
		long ts = 15 * 1000 * 60 * 60 * 24; // 15 days in ms
		initCleanupDir(cleanupRoot, ts);
		
		long cts = 20 * 1000 * 60 * 60 * 24; // 20 days in ms
		tfs.cleanupDir(cleanupRoot, cts, 10);
		
		ArrayList<String> files = tfs.listdirR(cleanupRoot);
		Collections.sort(files);
		assertEquals(4, files.size());
		assertTrue(files.get(0).endsWith("baz." + ts + ".gz"));
		assertTrue(files.get(1).endsWith("bongo." + ts + ".gz"));
		assertTrue(files.get(2).endsWith("foo." + ts + ".gz"));		
		assertTrue(files.get(3).endsWith("sd-foo." + ts + ".gz"));
		
		tfs.cleanupDir(cleanupRoot, cts, 4);
		files = tfs.listdirR(cleanupRoot);		
		assertEquals(0, files.size());	
		
		initCleanupDir(cleanupRoot, ts);
		tfs.cleanupDir(cleanupRoot, cts, 30);
		files = tfs.listdirR(cleanupRoot);		
		assertEquals(8, files.size());
		
		tfs.cleanupDir(cleanupRoot, cts, -1);
		files = tfs.listdirR(cleanupRoot);		
		assertEquals(8, files.size());
		
		tfs.cleanupDir(cleanupRoot, cts, -1867);
		files = tfs.listdirR(cleanupRoot);		
		assertEquals(8, files.size());
		
		tfs.cleanupDir(cleanupRoot, cts, 0);
		files = tfs.listdirR(cleanupRoot);		
		assertEquals(0, files.size());
				
		initCleanupDir(cleanupRoot, ts);
		
		tfs.cleanupDir("/invalid/dir", cts, 10);
		files = tfs.listdirR(cleanupRoot);		
		assertEquals(8, files.size());
		
		tfs.cleanupDir(cleanupRoot, 3, 1);
		files = tfs.listdirR(cleanupRoot);		
		assertEquals(8, files.size());
	}
	
	private void initCleanupDir(String cleanupRoot, long ts) throws IOException {
		Path testFile = new Path(OsPath.join(dataDir, "files/file1")); // on local FS
				
		tfs.mkdir(cleanupRoot);
		tfs.hdfs.copyFromLocalFile(testFile, new Path(OsPath.join(cleanupRoot, "foo.1.gz")));
		tfs.hdfs.copyFromLocalFile(testFile, new Path(OsPath.join(cleanupRoot, "bar.2.gz")));
		tfs.hdfs.copyFromLocalFile(testFile, new Path(OsPath.join(cleanupRoot, "foo." + ts + ".gz")));
		tfs.hdfs.copyFromLocalFile(testFile, new Path(OsPath.join(cleanupRoot, "baz." + ts + ".gz")));
		tfs.hdfs.copyFromLocalFile(testFile, new Path(OsPath.join(cleanupRoot, "bongo." + ts + ".gz")));
		
		tfs.hdfs.copyFromLocalFile(testFile, new Path(OsPath.join(cleanupRoot, "subdir/sd-foo.1.gz")));
		tfs.hdfs.copyFromLocalFile(testFile, new Path(OsPath.join(cleanupRoot, "subdir/sd-bar.2.gz")));
		tfs.hdfs.copyFromLocalFile(testFile, new Path(OsPath.join(cleanupRoot, "subdir/sd-foo." + ts + ".gz")));
	}
	
	@Test
	public void testCleanupMetaDir() throws IOException {	
		String cleanupRoot = OsPath.join(hdfsRoot, "cleanup");          // on HDFS
		Path testFile = new Path(OsPath.join(dataDir, "files/file1")); // on local FS
		
		long msPerDay = 1000 * 60 * 60 * 24;
		long ts1 = 15 * msPerDay; // 15 days in ms
		long ts2 = 20 * msPerDay; // 20 days in ms
		long ts3 = 25 * msPerDay; // 25 days in ms
		
		tfs.deleteDir(cleanupRoot);
		assertFalse(tfs.isdir(cleanupRoot));
		tfs.mkdir(cleanupRoot);
		tfs.hdfs.copyFromLocalFile(testFile, new Path(OsPath.join(cleanupRoot, "1.gz")));
		tfs.hdfs.copyFromLocalFile(testFile, new Path(OsPath.join(cleanupRoot, "2.gz")));
		tfs.hdfs.copyFromLocalFile(testFile, new Path(OsPath.join(cleanupRoot, ts1 + ".gz")));
		tfs.hdfs.copyFromLocalFile(testFile, new Path(OsPath.join(cleanupRoot, ts2 + ".gz")));
				
		tfs.cleanupMetaDir(cleanupRoot, ts3, 10);
		
		ArrayList<String> files = tfs.listdirR(cleanupRoot);
		Collections.sort(files);
		assertEquals(2, files.size());
		assertTrue(files.get(0).endsWith(ts1 + ".gz"));
		assertTrue(files.get(1).endsWith(ts2 + ".gz"));		
		
		tfs.cleanupMetaDir(cleanupRoot, ts3, 30);
		files = tfs.listdirR(cleanupRoot);		
		assertEquals(2, files.size());
		assertTrue(files.get(0).endsWith(ts1 + ".gz"));
		assertTrue(files.get(1).endsWith(ts2 + ".gz"));
		
		tfs.cleanupMetaDir(cleanupRoot, ts3, 1);
		files = tfs.listdirR(cleanupRoot);		
		assertEquals(1, files.size());
		assertTrue(files.get(0).endsWith(ts2 + ".gz"));
				
		tfs.cleanupMetaDir(cleanupRoot, ts3, 0);
		files = tfs.listdirR(cleanupRoot);		
		assertEquals(1, files.size());
		assertTrue(files.get(0).endsWith(ts2 + ".gz"));
		
		tfs.hdfs.copyFromLocalFile(testFile, new Path(OsPath.join(cleanupRoot, "1.gz")));
		tfs.hdfs.copyFromLocalFile(testFile, new Path(OsPath.join(cleanupRoot, "2.gz")));
		tfs.hdfs.copyFromLocalFile(testFile, new Path(OsPath.join(cleanupRoot, ts1 + ".gz")));
		tfs.hdfs.copyFromLocalFile(testFile, new Path(OsPath.join(cleanupRoot, ts2 + ".gz")));
		tfs.cleanupMetaDir(cleanupRoot, ts3, -1);
		files = tfs.listdirR(cleanupRoot);		
		Collections.sort(files);
		assertEquals(4, files.size());		
		
		tfs.cleanupMetaDir(cleanupRoot, ts3, -10);
		files = tfs.listdirR(cleanupRoot);		
		assertEquals(4, files.size());
		
		tfs.cleanupMetaDir("/invalid/dir", ts3, 10);
		files = tfs.listdirR(cleanupRoot);				
		assertEquals(4, files.size());
		
		tfs.cleanupMetaDir(cleanupRoot, 3, 1);
		files = tfs.listdirR(cleanupRoot);		
		assertEquals(4, files.size());
	}
	
	@Test
	public void testGetFilenameName() {
		assertEquals("foo", tfs.getFilenameName("hdfs://localhost/user/larsab/troilkatt/test/names/foo.123.gz"));
		assertEquals("foo", tfs.getFilenameName("/user/larsab/troilkatt/test/names/foo.123.gz"));
		assertNull(tfs.getFilenameName("troilkatt/test/names/foo.123"));
		assertNull(tfs.getFilenameName("troilkatt/test/names/foo"));
	}
	
	@Test
	public void testGetFilenameDir() {
		assertEquals("/user/larsab/troilkatt/test/names", tfs.getFilenameDir("hdfs://localhost/user/larsab/troilkatt/test/names/foo.123.gz"));
		assertEquals("/user/larsab/troilkatt/test/names", tfs.getFilenameDir("/user/larsab/troilkatt/test/names/foo.123.gz"));
		assertEquals("troilkatt/test/names", tfs.getFilenameDir("troilkatt/test/names/foo.123"));		
	}

	@Test
	public void testGetFilenameTimestamp() {
		assertEquals(123, tfs.getFilenameTimestamp("hdfs://localhost/user/larsab/troilkatt/test/names/foo.123.gz"));
		assertEquals(123, tfs.getFilenameTimestamp("/user/larsab/troilkatt/test/names/foo.123.gz"));
		assertEquals(-1, tfs.getFilenameTimestamp("troilkatt/test/names/foo.123"));
		assertEquals(-1, tfs.getFilenameTimestamp("troilkatt/test/names/foo"));
		assertEquals(-1, tfs.getFilenameTimestamp("troilkatt/test/names/foo.baz.none"));
	}

	@Test
	public void testGetFilenameCompression() {
		assertEquals("gz", tfs.getFilenameCompression("hdfs://localhost/user/larsab/troilkatt/test/Compressions/foo.123.gz"));
		assertEquals("gz", tfs.getFilenameCompression("/user/larsab/troilkatt/test/Compressions/foo.123.gz"));
		assertNull(tfs.getFilenameCompression("troilkatt/test/Compressions/foo.123"));
		assertNull(tfs.getFilenameCompression("troilkatt/test/Compressions/foo"));
	}

	@Test
	public void testGetDirTimestamp() {
		assertEquals(123, tfs.getDirTimestamp("hdfs://localhost/user/larsab/troilkatt/test/names/123.tar.gz"));
		assertEquals(123, tfs.getDirTimestamp("/user/larsab/troilkatt/test/names/123.tar.gz"));
		assertEquals(123, tfs.getDirTimestamp("troilkatt/test/names/123.zip"));		
		assertEquals(-1, tfs.getDirTimestamp("troilkatt/test/Compressions/foo.123"));
		assertEquals(-1, tfs.getDirTimestamp("troilkatt/test/Compressions/foo"));
	}

	@Test
	public void testGetDirCompression() {
		assertEquals("tar.gz", tfs.getDirCompression("hdfs://localhost/user/larsab/troilkatt/test/names/123.tar.gz"));
		assertEquals("tar.gz", tfs.getDirCompression("/user/larsab/troilkatt/test/names/123.tar.gz"));
		assertEquals("zip", tfs.getDirCompression("troilkatt/test/names/123.zip"));		
		assertEquals("123", tfs.getDirCompression("troilkatt/test/Compressions/foo.123"));
		assertNull(tfs.getDirCompression("troilkatt/test/Compressions/foo"));
	}

	@Test
	public void testIsValidCompression() {
		String[] valid = {"none", "gz", "bz2"};
		
		for (String s: valid) {
			assertTrue(TroilkattFS.isValidCompression(s));
		}
		assertFalse(TroilkattFS.isValidCompression("foo"));
	}

	// Getter not tested
	//@Test
	//public void testGetValidCompression() {
	//	
	//}
	
	@Test
	public void testListdirRecursive() throws IOException {		
		ArrayList<String> files = tfs.listdirR(OsPath.join(hdfsRoot, "ls"));
		
		assertEquals(6, files.size());
		Collections.sort(files);
		assertTrue(files.get(0).endsWith("file1"));
		assertTrue(files.get(1).endsWith("file2"));
		assertTrue(files.get(2).endsWith("file3"));
		assertTrue(files.get(3).endsWith("subdir1/file4"));
		assertTrue(files.get(4).endsWith("subdir1/file5"));
		assertTrue(files.get(5).endsWith("subdir2/file6"));
	}
	
	// Empty directory
	@Test
	public void testListdirRecursive2() throws IOException {
		ArrayList<String> files = tfs.listdirR(OsPath.join(hdfsRoot, "ls/subdir3"));
		assertEquals(0, files.size());		
	}
	
	// Invalid directory
	@Test
	public void testListdirRecursive3() throws IOException {		
		ArrayList<String> files = tfs.listdirR(OsPath.join(hdfsRoot, "ls/invalid-subdir"));
		assertNull(files);		
	}

	@Test
	public void testListdirNewest() throws IOException {
		ArrayList<String> files = tfs.listdirN(OsPath.join(hdfsRoot, "ts"));
		
		assertEquals(6, files.size());
		Collections.sort(files);
		assertTrue(files.get(0).endsWith("file1.3.gz"));
		assertTrue(files.get(1).endsWith("file2.3.bz2"));
		assertTrue(files.get(2).endsWith("file3.2.none"));
		assertTrue(files.get(3).endsWith("subdir1/file4.3.gz"));
		assertTrue(files.get(4).endsWith("subdir1/file5.2.none"));
		assertTrue(files.get(5).endsWith("subdir2/file6.2.none"));
	}
	
	// Empty directory
	@Test
	public void testListdirNewest2() throws IOException {		
		ArrayList<String> files = tfs.listdirN(OsPath.join(hdfsRoot, "ts/subdir3"));
		assertEquals(0, files.size());		
	}
	
	// Invalid directory
	@Test
	public void testListdirNewest3() throws IOException {
		ArrayList<String> files = tfs.listdirN(OsPath.join(hdfsRoot, "ts/invalid-subdir"));
		assertNull(files);		
	}

	@Test
	public void testListdirTimestamp() throws IOException {
		String cleanupRoot = OsPath.join(hdfsRoot, "cleanup");
		long ts1 = 15 * 1000 * 60 * 60 * 24; 
		
		/* Just resuse the cleanup file structure which is:
		   foo.1.gz
		   bar.2.gz
		   foo.ts1.gz
		   baz.ts1.gz
		   bongo.ts1.gz
		   subdir/sd-foo.1.gz
		   subdir/sd-bar.2.gz
	  	   subdir/sd-foo.ts1.hz
	  	*/
		initCleanupDir(cleanupRoot, ts1);
		
		ArrayList<String> files = tfs.listdirT(cleanupRoot, 1);
		Collections.sort(files);
		assertEquals(2, files.size());		
		assertTrue(files.get(0).endsWith("foo.1.gz"));
		assertTrue(files.get(1).endsWith("subdir/sd-foo.1.gz"));
				
		files = tfs.listdirT(cleanupRoot, ts1);
		Collections.sort(files);
		assertEquals(4, files.size());				
		assertTrue(files.get(0).endsWith("baz." + ts1 + ".gz"));
		assertTrue(files.get(1).endsWith("bongo." + ts1 + ".gz"));
		assertTrue(files.get(2).endsWith("foo." + ts1 + ".gz"));		
		assertTrue(files.get(3).endsWith("sd-foo." + ts1 + ".gz"));
				
		files = tfs.listdirT(cleanupRoot, ts1 + 1);
		assertTrue(files.isEmpty());
		
		files = tfs.listdirT("/invalid/dir", ts1);
		assertNull(files);
	}
	
	@Test
	public void testGetNewestDir() throws IOException {
		String dir = tfs.getNewestDir(OsPath.join(hdfsRoot, "tsd"));
		assertEquals("4.zip", dir);
	}

	// Invalid directory
	@Test
	public void testGetNewestDir2() throws IOException {
		String dir = tfs.getNewestDir(OsPath.join(hdfsRoot, "tsd-invalid"));
		assertNull(dir);
	}
	
	// Empty directory
	@Test
	public void testGetNewestDir3() throws IOException {
		String dir = tfs.getNewestDir(OsPath.join(hdfsRoot, "tsd-empty"));
		assertNull(dir);
	}
	
	@Test
	public void testGetFile() throws IOException {	
		String file = tfs.getFile(OsPath.join(hdfsRoot, "compressed-files/file1.1.gz"),
				outDir, tmpDir, logDir);
		assertTrue(file.endsWith(OsPath.join(outDir, "file1")));
		assertTrue(fileCmp(file, OsPath.join(dataDir, "files/file1")));
	}
	
	@Test
	public void testGetFile2() throws IOException {
		String file = tfs.getFile(OsPath.join(hdfsRoot, "compressed-files/file1.2.bz2"),
				outDir, tmpDir, logDir);
		assertTrue(file.endsWith(OsPath.join(outDir, "file1")));
		assertTrue(fileCmp(file, OsPath.join(dataDir, "files/file1")));
	}
	
	@Test
	public void testGetFile3() throws IOException {
		// Zip is not supported
		String file = tfs.getFile(OsPath.join(hdfsRoot, "compressed-files/file1.3.zip"),
				outDir, tmpDir, logDir);
		assertNull(file);
		//assertTrue(file.endsWith(OsPath.join(outDir, "file1")));
		//assertTrue(fileCmp(file, OsPath.join(dataDir, "files/file1")));
	}
	
	@Test
	public void testGetFile4() throws IOException {		
		String file = tfs.getFile(OsPath.join(hdfsRoot, "compressed-files/file1.4.none"),
				outDir, tmpDir, logDir);
		assertTrue(file.endsWith(OsPath.join(outDir, "file1")));
		assertTrue(fileCmp(file, OsPath.join(dataDir, "files/file1")));
	}
	
	// Invalid filename
	@Test
	public void testGetFile5() throws IOException {		
		String file = tfs.getFile(OsPath.join(hdfsRoot, "ls/file1"),
				outDir, tmpDir, logDir);
		assertNull(file);
	}
	
	// Invalid file
	@Test
	public void testGetFile6() throws IOException {
		String file = tfs.getFile(OsPath.join(hdfsRoot, "compressed-files/invalid.5.gz"),
				outDir, tmpDir, logDir);
		assertNull(file);
	}
	
	// Invalid local dir
	@Test
	public void testGetFile7() throws IOException {		
		String file = tfs.getFile(OsPath.join(hdfsRoot, "compressed-files/file1.1.gz"),
				"/foo/bar", tmpDir, logDir);
		assertNull(file);
	}
	
	// Invalid log dir
	//@Test
	public void testGetFile8() throws IOException {
		String file = tfs.getFile(OsPath.join(hdfsRoot, "compressed-files/file1.3.zip"),
				outDir, tmpDir, "/foo/bar");
		assertNull(file);
	}
	
	// Invalid tmp dir
	@Test
	public void testGetFile9() throws IOException {
		String file = tfs.getFile(OsPath.join(hdfsRoot, "compressed-files/file1.3.zip"),
				outDir, "/foo/bar", logDir);
		assertNull(file);
	}

	@Test
	public void testGetDirFiles() throws IOException {
		ArrayList<String> files = tfs.getDirFiles(OsPath.join(hdfsRoot, "compressed-dirs/1.tar.gz"),
				outDir, logDir, tmpDir);
		assertEquals(files.size(), 2);
		Collections.sort(files);
		assertTrue(files.get(0).equals(OsPath.join(outDir, "files/file1")));
		assertTrue(files.get(1).equals(OsPath.join(outDir, "files/file2")));
		assertTrue(fileCmp(files.get(0), OsPath.join(dataDir, "files/file1")));
		assertTrue(fileCmp(files.get(1), OsPath.join(dataDir, "files/file2")));
	}

	@Test
	public void testGetDirFiles2() throws IOException {
		ArrayList<String> files = tfs.getDirFiles(OsPath.join(hdfsRoot, "compressed-dirs/2.tar.bz2"),
				outDir, logDir, tmpDir);
		assertEquals(files.size(), 2);
		Collections.sort(files);
		assertTrue(files.get(0).endsWith(OsPath.join(outDir, "files/file1")));
		assertTrue(files.get(1).endsWith(OsPath.join(outDir, "files/file2")));
		assertTrue(fileCmp(files.get(0), OsPath.join(dataDir, "files/file1")));
		assertTrue(fileCmp(files.get(1), OsPath.join(dataDir, "files/file2")));
	}

	// Zip compression not supported for directories
	@Test
	public void testGetDirFiles3() throws IOException {	
		ArrayList<String> files = tfs.getDirFiles(OsPath.join(hdfsRoot, "compressed-dirs/3.zip"),
				outDir, logDir, tmpDir);
		assertNull(files);
		//assertEquals(files.size(), 2);
		//Collections.sort(files);
		//assertTrue(files.get(0).endsWith(OsPath.join(outDir, "files/file1")));
		//assertTrue(files.get(1).endsWith(OsPath.join(outDir, "files/file2")));
		//assertTrue(fileCmp(files.get(0), OsPath.join(dataDir, "files/file1")));
		//assertTrue(fileCmp(files.get(1), OsPath.join(dataDir, "files/file2")));
	}

	@Test
	public void testGetDirFiles4() throws IOException {		
		ArrayList<String> files = tfs.getDirFiles(OsPath.join(hdfsRoot, "compressed-dirs/4.none"),
				outDir, logDir, tmpDir);
		assertEquals(files.size(), 2);
		Collections.sort(files);
		assertTrue(files.get(0).endsWith(OsPath.join(outDir, "file1")));
		assertTrue(files.get(1).endsWith(OsPath.join(outDir, "file2")));
		assertTrue(fileCmp(files.get(0), OsPath.join(dataDir, "files/file1")));
		assertTrue(fileCmp(files.get(1), OsPath.join(dataDir, "files/file2")));
	}

	// Non-existing HDFS dir
	@Test
	public void testGetDirFiles5() throws IOException {	
		ArrayList<String> files = tfs.getDirFiles(OsPath.join(hdfsRoot, "compressed-dirs/3242.none"),
				outDir, logDir, tmpDir);
		assertNull(files);		
	}

	// Invalid file
	@Test
	public void testGetDirFiles6() throws IOException {		
		ArrayList<String> files = tfs.getDirFiles(OsPath.join(hdfsRoot, "compressed-dirs/5.zip"),
				outDir, logDir, tmpDir);
		assertNull(files);		
	}

	// Invalid local dir
	@Test(expected=IOException.class)
	public void testGetDirFiles7() throws IOException {
		ArrayList<String> files = tfs.getDirFiles(OsPath.join(hdfsRoot, "compressed-dirs/2.tar.bz2"),
				"/foo/bar", logDir, tmpDir);
		assertNull(files);		
	}

	// Invalid log dir
	@Test
	public void testGetDirFiles8() throws IOException {
		ArrayList<String> files = tfs.getDirFiles(OsPath.join(hdfsRoot, "compressed-dirs/2.tar.bz2"),
				outDir, "/foo/bar", tmpDir);
		assertNull(files);		
	}

	// Invalid tmp dir
	@Test
	public void testGetDirFiles9() throws IOException {		
		ArrayList<String> files = tfs.getDirFiles(OsPath.join(hdfsRoot, "compressed-dirs/2.tar.bz2"),
				outDir, tmpDir, "/foo/bar");
		assertNull(files);		
	}

	@Test
	public void testPutFile() throws IOException {
		String houtDir = OsPath.join(hdfsRoot, "out");
		hdfs.mkdirs(new Path(houtDir));
		
		String srcFile = OsPath.join(tmpDir, "file1");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);		
		String hdfsName = tfs.putLocalFile(srcFile, houtDir, tmpDir, logDir, "gz", 1);
		assertEquals(OsPath.join(houtDir, "file1.1.gz"), hdfsName);
		String localName = tfs.getFile(hdfsName, outDir, tmpDir, logDir);
		assertNotNull(localName);
		assertTrue(localName.endsWith(OsPath.join(outDir, "file1")));
		assertTrue(fileCmp(localName, OsPath.join(dataDir, "files/file1")));
		
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);
		hdfsName = tfs.putLocalFile(srcFile, houtDir, tmpDir, logDir, "bz2", 2);
		assertEquals(OsPath.join(houtDir, "file1.2.bz2"), hdfsName);
		localName = tfs.getFile(hdfsName, outDir, tmpDir, logDir);
		assertTrue(localName.endsWith(OsPath.join(outDir, "file1")));
		assertTrue(fileCmp(localName, OsPath.join(dataDir, "files/file1")));
		
		// Zip compression is not supported
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);
		hdfsName = tfs.putLocalFile(srcFile, houtDir, tmpDir, logDir, "zip", 3);
		assertNull(hdfsName);
		//assertEquals(OsPath.join(houtDir, "file1.3.zip"), hdfsName);
		//localName = tfs.getFile(hdfsName, outDir, tmpDir, logDir);
		//assertTrue(localName.endsWith(OsPath.join(outDir, "file1")));
		//assertTrue(fileCmp(localName, OsPath.join(dataDir, "files/file1")));
		
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);
		hdfsName = tfs.putLocalFile(srcFile, houtDir, tmpDir, logDir, "none", 4);
		assertEquals(OsPath.join(houtDir, "file1.4.none"), hdfsName);
		localName = tfs.getFile(hdfsName, outDir, tmpDir, logDir); 
		assertTrue(localName.endsWith(OsPath.join(outDir, "file1")));
		assertTrue(fileCmp(localName, OsPath.join(dataDir, "files/file1")));		
	}
	
	// Invalid local filename
	@Test
	public void testPutFile2() throws IOException {
		String houtDir = OsPath.join(hdfsRoot, "out");
		hdfs.mkdirs(new Path(houtDir));
		
		String hdfsName = tfs.putLocalFile(OsPath.join(dataDir, "files/non-existing"), houtDir, tmpDir, logDir, "gz", 1);
		assertNull(hdfsName);
	}
	
	// Invalid HDFS dir filename
	// Permissions do not work correctly in pseudo mode
	//@Test
	//public void testPutFile3() throws IOException {
	//	String srcFile = OsPath.join(tmpDir, "file1");
	//	OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);		
	//	String hdfsName = tfs.putFile(srcFile, "/foo/bar", tmpDir, logDir, "gz", 1);
	//	assertNull(hdfsName);
	//}
	
	// Invalid tmp dir
	@Test
	public void testPutFile4() throws IOException {
		String houtDir = OsPath.join(hdfsRoot, "out");
		hdfs.mkdirs(new Path(houtDir));
		
		String srcFile = OsPath.join(tmpDir, "file1");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);		
		String hdfsName = tfs.putLocalFile(srcFile, houtDir, "/foo", logDir, "zip", 1);
		assertNull(hdfsName);
	}
	
	// Invalid compresison
	@Test
	public void testPutFile5() throws IOException {
		String houtDir = OsPath.join(hdfsRoot, "out");
		hdfs.mkdirs(new Path(houtDir));
		
		String srcFile = OsPath.join(tmpDir, "file1");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);		
		String hdfsName = tfs.putLocalFile(srcFile, houtDir, tmpDir, logDir, "foo", 1);
		assertNull(hdfsName);
	}
	
	// Invalid timestamp
	@Test
	public void testPutFile6() throws IOException {
		String houtDir = OsPath.join(hdfsRoot, "out");
		hdfs.mkdirs(new Path(houtDir));
		
		String srcFile = OsPath.join(tmpDir, "file1");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);		
		String hdfsName = tfs.putLocalFile(srcFile, houtDir, tmpDir, logDir, "gz", -321);
		assertNull(hdfsName);
	}
	
	// Invalid log dir
	@Test
	public void testPutFile7() throws IOException {
		String houtDir = OsPath.join(hdfsRoot, "out");
		hdfs.mkdirs(new Path(houtDir));
		
		String srcFile = OsPath.join(tmpDir, "file1");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);		
		String hdfsName = tfs.putLocalFile(srcFile, houtDir, tmpDir, "/foo/bar", "zip", 1);
		assertNull(hdfsName);
	}
	
	@Test
	public void testPutFile8() throws IOException {
		String houtDir = OsPath.join(hdfsRoot, "out");
		hdfs.mkdirs(new Path(houtDir));
		
		String srcFile = OsPath.join(tmpDir, "file1");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);		
		String hdfsName = tfs.putLocalFile(srcFile, houtDir, tmpDir, logDir, "gz");
		assertEquals(OsPath.join(houtDir, "file1.gz"), hdfsName);
		
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);
		hdfsName = tfs.putLocalFile(srcFile, houtDir, tmpDir, logDir, "bz2");
		assertEquals(OsPath.join(houtDir, "file1.bz2"), hdfsName);
		
		// Zip compression is not supported
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);
		hdfsName = tfs.putLocalFile(srcFile, houtDir, tmpDir, logDir, "zip");
		assertNull(hdfsName);
		//assertEquals(OsPath.join(houtDir, "file1.zip"), hdfsName);
		
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);
		hdfsName = tfs.putLocalFile(srcFile, houtDir, tmpDir, logDir, "none");
		assertEquals(OsPath.join(houtDir, "file1.none"), hdfsName);
	}

	// Invalid local filename
	@Test
	public void testPutFile9() throws IOException {
		String houtDir = OsPath.join(hdfsRoot, "out");
		hdfs.mkdirs(new Path(houtDir));
		
		String hdfsName = tfs.putLocalFile(OsPath.join(dataDir, "files/non-existing"), houtDir, tmpDir, logDir, "gz");
		assertNull(hdfsName);
	}
	
	// Invalid HDFS dir filename
	// Permissions do not work correctly in pseudo mode
	//@Test
	//public void testPutFile10() throws IOException {
	//	String srcFile = OsPath.join(tmpDir, "file1");
	//	OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);		
	//	String hdfsName = tfs.putFile(srcFile, "/foo/bar", tmpDir, logDir, "gz");
	//	assertNull(hdfsName);
	//}
	
	// Invalid tmp dir
	@Test
	public void testPutFile11() throws IOException {
		String houtDir = OsPath.join(hdfsRoot, "out");
		hdfs.mkdirs(new Path(houtDir));
		
		String srcFile = OsPath.join(tmpDir, "file1");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);		
		String hdfsName = tfs.putLocalFile(srcFile, houtDir, "/foo", logDir, "zip");
		assertNull(hdfsName);
	}
	
	// Invalid log dir
	@Test
	public void testPutFile12() throws IOException {
		String houtDir = OsPath.join(hdfsRoot, "out");
		hdfs.mkdirs(new Path(houtDir));
		
		String srcFile = OsPath.join(tmpDir, "file1");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);		
		String hdfsName = tfs.putLocalFile(srcFile, houtDir, tmpDir, "/foo/bar", "zip");
		assertNull(hdfsName);
	}
	
	// Invalid compresison
	@Test
	public void testPutFile13() throws IOException {
		String houtDir = OsPath.join(hdfsRoot, "out");
		hdfs.mkdirs(new Path(houtDir));
		
		String srcFile = OsPath.join(tmpDir, "file1");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);		
		String hdfsName = tfs.putLocalFile(srcFile, houtDir, tmpDir, logDir, "foo");
		assertNull(hdfsName);
	}
	
	// Empty file
	@Test
	public void testPutFile14() throws IOException {
		String houtDir = OsPath.join(hdfsRoot, "out");
		hdfs.mkdirs(new Path(houtDir));
		
		String srcFile = OsPath.join(tmpDir, "empty");
		byte[] empty = new byte[0];
		FSUtils.writeFile(srcFile, empty);
		
		String hdfsName = tfs.putLocalFile(srcFile, houtDir, tmpDir, logDir, "foo");
		assertNull(hdfsName);
	}
	
	@Test
	public void testPutDirFiles() throws IOException {
		String houtDir = OsPath.join(hdfsRoot, "out");
		hdfs.mkdirs(new Path(houtDir));
		
		// Zip compression is not supported
		assertFalse( tfs.putLocalDirFiles(houtDir, 10, localFiles, "zip", logDir, tmpDir) );
		//assertTrue( tfs.isfile(OsPath.join(houtDir, "10.zip")));
		//OsPath.delete(outDir);
		//OsPath.mkdir(outDir);
		//ArrayList<String> files = tfs.getDirFiles(OsPath.join(houtDir, "10.zip"),
		//		outDir, logDir, tmpDir);
		//Collections.sort(files);		
		//assertTrue(fileCmp(files.get(0), OsPath.join(dataDir, "files/file1")));
		//assertTrue(fileCmp(files.get(1), OsPath.join(dataDir, "files/file2")));
		
		assertTrue( tfs.putLocalDirFiles(houtDir, 11, localFiles, "tar.gz", logDir, tmpDir) );
		assertTrue( tfs.isfile(OsPath.join(houtDir, "11.tar.gz")));
		OsPath.delete(outDir);
		OsPath.mkdir(outDir);
		ArrayList<String> files = tfs.getDirFiles(OsPath.join(houtDir, "11.tar.gz"),
				outDir, logDir, tmpDir);
		Collections.sort(files);		
		assertTrue(fileCmp(files.get(0), OsPath.join(dataDir, "files/file1")));
		assertTrue(fileCmp(files.get(1), OsPath.join(dataDir, "files/file2")));
		
		assertTrue( tfs.putLocalDirFiles(houtDir, 12, localFiles, "tar.bz2", logDir, tmpDir) );
		assertTrue( tfs.isfile(OsPath.join(houtDir, "12.tar.bz2")));
		OsPath.delete(outDir);
		OsPath.mkdir(outDir);
		files = tfs.getDirFiles(OsPath.join(houtDir, "12.tar.bz2"),
				outDir, logDir, tmpDir);
		Collections.sort(files);		
		assertTrue(fileCmp(files.get(0), OsPath.join(dataDir, "files/file1")));
		assertTrue(fileCmp(files.get(1), OsPath.join(dataDir, "files/file2")));
		
		assertTrue( tfs.putLocalDirFiles(houtDir, 13, localFiles, "none", logDir, tmpDir) );
		assertTrue( tfs.isdir(OsPath.join(houtDir, "13.none")));
		OsPath.delete(outDir);
		OsPath.mkdir(outDir);
		files = tfs.getDirFiles(OsPath.join(houtDir, "13.none"),
				outDir, logDir, tmpDir);
		Collections.sort(files);		
		assertTrue(fileCmp(files.get(0), OsPath.join(dataDir, "files/file1")));
		assertTrue(fileCmp(files.get(1), OsPath.join(dataDir, "files/file2")));
	}
	
	// Non-existing HDFS dir
	@Test
	public void testPutDirFiles2() throws IOException {
		assertFalse( tfs.putLocalDirFiles("/foo/bar", 10, localFiles, "zip", logDir, tmpDir) );
	}
	
	// Invalid timestamp
	@Test
	public void testPutDirFiles3() throws IOException {
		String houtDir = OsPath.join(hdfsRoot, "out");
		hdfs.mkdirs(new Path(houtDir));
		
		assertFalse( tfs.putLocalDirFiles(houtDir, -10, localFiles, "zip", logDir, tmpDir) );
	}
	
	// Invalid local file
	@Test
	public void testPutDirFiles4() throws IOException {
		String houtDir = OsPath.join(hdfsRoot, "out");
		hdfs.mkdirs(new Path(houtDir));
		localFiles.add("/foo/bar/baz");
		assertFalse( tfs.putLocalDirFiles(houtDir, 10, localFiles, "zip", logDir, tmpDir) );
	}
	
	// No local files
	@Test
	public void testPutDirFiles5() throws IOException {
		String houtDir = OsPath.join(hdfsRoot, "out");
		hdfs.mkdirs(new Path(houtDir));
		localFiles.clear();
		assertFalse( tfs.putLocalDirFiles(houtDir, 10, localFiles, "zip", logDir, tmpDir) );
	}
	
	// Invalid compression
	@Test
	public void testPutDirFiles6() throws IOException {
		String houtDir = OsPath.join(hdfsRoot, "out");
		hdfs.mkdirs(new Path(houtDir));
		
		assertFalse( tfs.putLocalDirFiles(houtDir, 10, localFiles, "foo", logDir, tmpDir) );
	}
	
	// Invalid log dir
	@Test
	public void testPutDirFiles7() throws IOException {
		String houtDir = OsPath.join(hdfsRoot, "out");
		hdfs.mkdirs(new Path(houtDir));
		
		assertFalse( tfs.putLocalDirFiles(houtDir, 10, localFiles, "zip", "/doo/bar", tmpDir) );
	}
	
	// Invalid tmpDir
	@Test
	public void testPutDirFiles8() throws IOException {
		String houtDir = OsPath.join(hdfsRoot, "out");
		hdfs.mkdirs(new Path(houtDir));
		
		assertFalse( tfs.putLocalDirFiles(houtDir, 10, localFiles, "zip", logDir, "/doo/bna") );
	}

	@Test
	public void testMoveFile() throws IOException {
		String srcFile = OsPath.join(hdfsRoot, "moveSrc/file1");
		String dstDir = OsPath.join(hdfsRoot, "moveDst");
		String dstFile = OsPath.join(hdfsRoot, "moveDst/file1.4.none");
		
		/*
		 * Prepare
		 */
		tfs.deleteDir(dstDir);
		tfs.mkdir(dstDir);
		hdfs.copyFromLocalFile(false, true,
				new Path(OsPath.join(dataDir, "files/file1")),
				new Path(srcFile));		
		assertTrue(tfs.isfile(srcFile));
		assertFalse(tfs.isfile(dstFile));				 
		assertEquals(dstFile, tfs.putHDFSFile(srcFile, dstDir, tmpDir, logDir, "none", 4));		
		assertFalse(tfs.isfile(srcFile));
		assertTrue(tfs.isfile(dstFile));
		
		dstFile = OsPath.join(hdfsRoot, "moveDst/file1.5.gz");
		hdfs.copyFromLocalFile(false, true,
				new Path(OsPath.join(dataDir, "files/file1")),
				new Path(srcFile));		
		assertTrue(tfs.isfile(srcFile));
		assertFalse(tfs.isfile(dstFile));				 
		assertEquals(dstFile, tfs.putHDFSFile(srcFile, dstDir, tmpDir, logDir, "gz", 5));		
		assertFalse(tfs.isfile(srcFile));
		assertTrue(tfs.isfile(dstFile));
		
		dstFile = OsPath.join(hdfsRoot, "moveDst/file1.6.bz2");
		hdfs.copyFromLocalFile(false, true,
				new Path(OsPath.join(dataDir, "files/file1")),
				new Path(srcFile));		
		assertTrue(tfs.isfile(srcFile));
		assertFalse(tfs.isfile(dstFile));				 
		assertEquals(dstFile, tfs.putHDFSFile(srcFile, dstDir, tmpDir, logDir, "bz2", 6));		
		assertFalse(tfs.isfile(srcFile));
		assertTrue(tfs.isfile(dstFile));
		
		srcFile = OsPath.join(hdfsRoot, "moveSrc/file1.gz");
		// Move back to source directory
		assertTrue(tfs.hdfs.rename(new Path(OsPath.join(hdfsRoot, "moveDst/file1.5.gz")), 
				new Path(srcFile)));		
		dstFile = OsPath.join(hdfsRoot, "moveDst/file1.7.gz");				
		assertTrue(tfs.isfile(srcFile));
		assertFalse(tfs.isfile(dstFile));				 
		assertEquals(dstFile, tfs.putHDFSFile(srcFile, dstDir, tmpDir, logDir, "gz", 7));		
		assertFalse(tfs.isfile(srcFile));
		assertTrue(tfs.isfile(dstFile));
		
		// Move back to source directory
		assertTrue(tfs.hdfs.rename(new Path(dstFile), 
				new Path(srcFile)));		
		dstFile = OsPath.join(hdfsRoot, "moveDst/subdir/file1.9.gz");				
		assertTrue(tfs.isfile(srcFile));
		assertFalse(tfs.isfile(dstFile));				 
		assertEquals(dstFile, tfs.putHDFSFile(srcFile, OsPath.join(dstDir, "subdir"),
				tmpDir, logDir, "gz", 9));		
		assertFalse(tfs.isfile(srcFile));
		assertTrue(tfs.isfile(dstFile));
				
		// Move back to source directory
		assertTrue(tfs.hdfs.rename(new Path(dstFile), 
				new Path(srcFile)));		
		dstFile = OsPath.join(hdfsRoot, "moveDst/file1.gz.8.bz2");				
		assertTrue(tfs.isfile(srcFile));
		assertFalse(tfs.isfile(dstFile));				 
		assertEquals(dstFile, tfs.putHDFSFile(srcFile, dstDir, tmpDir, logDir, "bz2", 8));		
		assertFalse(tfs.isfile(srcFile));
		assertTrue(tfs.isfile(dstFile));
		
		// Overwrite file
		srcFile = OsPath.join(hdfsRoot, "moveSrc/file1");		
		dstFile = OsPath.join(hdfsRoot, "moveDst/file1.10.none");
		hdfs.copyFromLocalFile(false, true,
				new Path(OsPath.join(dataDir, "files/file1")),
				new Path(srcFile));		
		hdfs.copyFromLocalFile(false, true,
				new Path(OsPath.join(dataDir, "files/file1")),
				new Path(dstFile));		
		assertTrue(tfs.isfile(srcFile));
		assertTrue(tfs.isfile(dstFile));
		assertEquals(dstFile, tfs.putHDFSFile(srcFile, dstDir, tmpDir, logDir, "none", 10));
		assertFalse(tfs.isfile(srcFile));
		assertTrue(tfs.isfile(dstFile));
		
		// Invalid source file
		srcFile = "/invalid/dir/filename";
		dstFile = OsPath.join(hdfsRoot, "moveDst/file1.9.gz");				
		assertFalse(tfs.isfile(srcFile));
		assertFalse(tfs.isfile(dstFile));				 
		assertNull(tfs.putHDFSFile(srcFile, dstDir, tmpDir, logDir, "gz", 9));				
		assertFalse(tfs.isfile(dstFile));

		srcFile = OsPath.join(hdfsRoot, "moveSrc/file1");
		dstFile = OsPath.join(hdfsRoot, "moveDst/file1.9.gz");
		hdfs.copyFromLocalFile(false, true,
				new Path(OsPath.join(dataDir, "files/file1")),
				new Path(srcFile));
				
		// Invalid compression format
		assertTrue(tfs.isfile(srcFile));
		assertFalse(tfs.isfile(dstFile));
		assertNull(tfs.putHDFSFile(srcFile, dstDir, tmpDir, logDir, "invalid", 10));
		assertTrue(tfs.isfile(srcFile));
		
		// Invalid timestamp
		assertTrue(tfs.isfile(srcFile));
		assertFalse(tfs.isfile(dstFile));
		assertNull(tfs.putHDFSFile(srcFile, dstDir, tmpDir, logDir, "bz2", -10));
		assertTrue(tfs.isfile(srcFile));				
	}
	
	/* Invalid destination directory
	 *
	 * Note! This test can fail when HDFS is run in pseudo mode since it may not support
	 * file permissions.
	 */
	@Test(expected=IOException.class) 
	public void testMoveFileI1() throws IOException {
		String srcFile = OsPath.join(hdfsRoot, "moveSrc/file1");
		String dstFile = OsPath.join(hdfsRoot, "moveDst/file1.9.gz");
		hdfs.copyFromLocalFile(false, true,
				new Path(OsPath.join(dataDir, "files/file1")),
				new Path(srcFile));			
		assertTrue(tfs.isfile(srcFile));
		assertFalse(tfs.isfile(dstFile));
		assertNull(tfs.putHDFSFile(srcFile, "/foo/bar/baz", tmpDir, logDir, "gz", 9));
		assertTrue(tfs.isfile(srcFile));
	}
	
	/*
	 * Below two tests not run since there is no compression codec supported by Troilkatt that is not also
	 * supported by hadoop.
	 */
	// Invalid tmp directory
	//@Test(expected=IOException.class) 
	//public void testMoveFileI2() throws IOException {
	//	String srcFile = OsPath.join(testRoot, "moveSrc/file1");
	//	hdfs.copyFromLocalFile(false, true,
	//			new Path(OsPath.join(dataDir, "files/file1")),
	//			new Path(srcFile));			
	//	String dstDir = OsPath.join(testRoot, "moveDst");
	//	String dstFile = OsPath.join(testRoot, "moveDst/file1.10.bz2");
	//	assertTrue(tfs.isfile(srcFile));
	//	assertFalse(tfs.isfile(dstFile));
	//	tfs.putHDFSFile(srcFile, dstDir, "/foo/bar", logDir, "bz2", 10);
	//	assertTrue(tfs.isfile(srcFile));
	//}
	
	// Invalid log directory
	//@Test(expected=IOException.class) 
	//public void testMoveFileI3() throws IOException {
	//	String srcFile = OsPath.join(testRoot, "moveSrc/file1");
	//	hdfs.copyFromLocalFile(false, true,
	//			new Path(OsPath.join(dataDir, "files/file1")),
	//			new Path(srcFile));			
	//	String dstDir = OsPath.join(testRoot, "moveDst");
	//	String dstFile = OsPath.join(testRoot, "moveDst/file1.11.bz2");
	//	assertTrue(tfs.isfile(srcFile));
	//	assertFalse(tfs.isfile(dstFile));
	//	tfs.putHDFSFile(srcFile, dstDir, tmpDir, "/foo/bar", "bz2", 10);
	//	assertTrue(tfs.isfile(srcFile));
	//}
	
	
	@Test
	public void testMoveFile2() throws IOException {
		String localFile1 = OsPath.join(dataDir, "files/file1");
		String localFile2 = OsPath.join(tmpDir, "file1");
		String srcDir = OsPath.join(hdfsRoot, "moveSrc");
		String dstDir = OsPath.join(hdfsRoot, "moveDst");		
		
		tfs.deleteDir(dstDir);
		tfs.mkdir(dstDir);
		OsPath.copy(localFile1, localFile2);
		assertEquals(OsPath.join(srcDir, "file1.4.none"),
				tfs.putLocalFile(localFile2, srcDir, tmpDir, logDir, "none", 4));
		OsPath.copy(localFile1, localFile2);
		assertEquals(OsPath.join(srcDir, "file1.5.gz"),
				tfs.putLocalFile(localFile2, srcDir,	tmpDir, logDir, "gz", 5));
		OsPath.copy(localFile1, localFile2);
		assertEquals(OsPath.join(srcDir, "file1.6.bz2"),
				tfs.putLocalFile(localFile2, srcDir,	tmpDir, logDir, "bz2", 6));
		
		String srcFile = OsPath.join(srcDir, "file1.4.none");
		String dstFile = OsPath.join(dstDir, "file1.4.none");
		assertTrue(tfs.isfile(srcFile));
		assertFalse(tfs.isfile(dstFile));
		assertEquals(dstFile, tfs.putHDFSFile(srcFile, dstDir));
		assertFalse(tfs.isfile(srcFile));
		assertTrue(tfs.isfile(dstFile));
		
		srcFile = OsPath.join(srcDir, "file1.5.gz");
		dstFile = OsPath.join(dstDir, "file1.5.gz");
		assertTrue(tfs.isfile(srcFile));
		assertFalse(tfs.isfile(dstFile));
		assertEquals(dstFile, tfs.putHDFSFile(srcFile, dstDir));
		assertFalse(tfs.isfile(srcFile));
		assertTrue(tfs.isfile(dstFile));
		
		srcFile = OsPath.join(srcDir, "file1.6.bz2");
		dstFile = OsPath.join(dstDir, "file1.6.bz2");
		assertTrue(tfs.isfile(srcFile));
		assertFalse(tfs.isfile(dstFile));
		assertEquals(dstFile, tfs.putHDFSFile(srcFile, dstDir));
		assertFalse(tfs.isfile(srcFile));
		assertTrue(tfs.isfile(dstFile));
		
		// Move file back to src
		OsPath.copy(localFile1, localFile2);
		assertEquals(OsPath.join(srcDir, "file1.7.gz"),
				tfs.putLocalFile(localFile2, srcDir, tmpDir, logDir, "gz", 7));
		srcFile = OsPath.join(srcDir, "file1.7.gz");
		dstFile = OsPath.join(dstDir, "subdir/file1.7.gz");
		assertTrue(tfs.isfile(srcFile));
		assertFalse(tfs.isfile(dstFile));
		assertEquals(dstFile, tfs.putHDFSFile(srcFile, OsPath.join(dstDir, "subdir")));
		assertFalse(tfs.isfile(srcFile));
		assertTrue(tfs.isfile(dstFile));
		
		// Invalid source file
		assertNull(tfs.putHDFSFile("/invalud/file", dstDir));
		
		// Missing timestamp
		srcFile = OsPath.join(dstDir, "file1.gz");
		OsPath.copy(localFile1, localFile2);
		tfs.hdfs.moveFromLocalFile(new Path(localFile2), new Path(srcFile));
		assertTrue(tfs.isfile(srcFile));
		assertNull(tfs.putHDFSFile(srcFile, dstDir));
		
		// Missing compression
		srcFile = OsPath.join(dstDir, "file1.5");
		OsPath.copy(localFile1, localFile2);
		tfs.hdfs.moveFromLocalFile(new Path(localFile2), new Path(srcFile));
		assertTrue(tfs.isfile(srcFile));
		assertNull(tfs.putHDFSFile(srcFile, dstDir));
		
		// Missing timestamp and compression		
		srcFile = OsPath.join(dstDir, "file1");
		OsPath.copy(localFile1, localFile2);
		tfs.hdfs.moveFromLocalFile(new Path(localFile2), new Path(srcFile));
		assertTrue(tfs.isfile(srcFile));
		assertNull(tfs.putHDFSFile(srcFile, dstDir));
		
		// Invalid destination directory
		// Missing timestamp
		srcFile = OsPath.join(dstDir, "file1.8.gz");
		OsPath.copy(localFile1, localFile2);
		tfs.hdfs.moveFromLocalFile(new Path(localFile2), new Path(srcFile));
		assertTrue(tfs.isfile(srcFile));
		// Note fails since pseudo mode does not respect directory user flags
		//assertNull(tfs.putHDFSFile(srcFile, "/invalid/dir"));
	}
	
	@Test
	public void testRenameFile() throws IOException {
		String srcFile = OsPath.join(hdfsRoot, "moveSrc/file1");
		String dstDir = OsPath.join(hdfsRoot, "moveDst");
		String dstFile = OsPath.join(hdfsRoot, "moveDst/file1");
		
		/*
		 * Prepare
		 */
		tfs.deleteDir(dstDir);
		tfs.mkdir(dstDir);
		hdfs.copyFromLocalFile(false, true,
				new Path(OsPath.join(dataDir, "files/file1")),
				new Path(srcFile));		
		assertTrue(tfs.isfile(srcFile));
		assertFalse(tfs.isfile(dstFile));				 
		
		
		// Valid move
		assertTrue(tfs.renameFile(srcFile, dstFile));
		assertFalse(tfs.isfile(srcFile));
		assertTrue(tfs.isfile(dstFile));
		
		// Also valid, even if dest already exists
		hdfs.copyFromLocalFile(false, true,
				new Path(OsPath.join(dataDir, "files/file1")),
				new Path(srcFile));
		assertTrue(tfs.renameFile(srcFile, dstFile));
		assertFalse(tfs.isfile(srcFile));
		assertTrue(tfs.isfile(dstFile));
		
		// Invalid since source does not exist
		assertFalse(tfs.renameFile(srcFile, dstFile));
		assertTrue(tfs.isfile(dstFile));
		
		assertTrue(tfs.renameFile(dstFile, srcFile));
		// Invalid since output directory does not exist
		assertFalse(tfs.renameFile(srcFile, "/foo"));
	}
	
	@Test
	public void testFileSize() throws IOException {
		String filename = OsPath.join(hdfsRoot, "ls/file1");		
		hdfs.copyFromLocalFile(false, true,
				new Path(OsPath.join(dataDir, "files/file1")),
				new Path(filename));		
		assertTrue(tfs.isfile(filename));
				
		assertEquals(8396983, tfs.fileSize(filename));
		
		String srcFile = OsPath.join(tmpDir, "empty");
		byte[] empty = new byte[0];
		FSUtils.writeFile(srcFile, empty);
		tfs.mkdir(OsPath.join(hdfsRoot, "tmp"));
		hdfs.copyFromLocalFile(false, true, new Path(srcFile), new Path(OsPath.join(hdfsRoot, "tmp/empty")));		
		assertEquals(0,tfs.fileSize( OsPath.join(hdfsRoot, "tmp/empty")));
	}
}
