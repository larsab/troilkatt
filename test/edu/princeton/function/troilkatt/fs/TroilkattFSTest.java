package edu.princeton.function.troilkatt.fs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.princeton.function.troilkatt.TestSuper;
import edu.princeton.function.troilkatt.Troilkatt;
import edu.princeton.function.troilkatt.TroilkattProperties;

public class TroilkattFSTest extends TestSuper {
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
		troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));				
		tfs = new TroilkattFS();
		
		OsPath.deleteAll(tmpDir);
		OsPath.mkdir(tmpDir);
		OsPath.deleteAll(logDir);
		OsPath.mkdir(logDir);
		OsPath.deleteAll(outDir);
		OsPath.mkdir(outDir);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testTroilkattFS() {				
		assertNotNull(tfs.logger);
	}

	@Test
	public void testGetFilenameName() {
		assertEquals("foo", tfs.getFilenameName("hdfs://localhost/user/larsab/troilkatt/test/names/foo.123.gz"));
		assertEquals("foo", tfs.getFilenameName("/user/larsab/troilkatt/test/names/foo.123.gz"));
		assertNull(tfs.getFilenameName("troilkatt/test/names/foo.123"));
		assertNull(tfs.getFilenameName("troilkatt/test/names/foo"));
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
	//@Test
	//public void testCompressFile4() throws IOException {
	//	String compressedName = tfs.compressFile(OsPath.join(dataDir, "files/file1"), tmpDir, "/foo", "gz");
	//	assertNull(compressedName);
	//}
	
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
	//@Test
	//public void testUncompressFile6() {
	//	assertFalse( tfs.uncompressFile(OsPath.join(dataDir, "files/file1.1.gz"), 
	//			OsPath.join(tmpDir, "file1"), "/foo") );
	//}
	
	@Test
	public void testCompressUncompressDirectory() throws IOException {	
		OsPath.delete(outDir);
		OsPath.mkdir(outDir);
		OsPath.copy(OsPath.join(dataDir, "files/file1"), OsPath.join(outDir, "file1"));
		OsPath.copy(OsPath.join(dataDir, "files/file2"), OsPath.join(outDir, "file2"));
		String compressedDir = tfs.compressDirectory(outDir, OsPath.join(tmpDir, "output"), logDir, "tar.gz");
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
		compressedDir = tfs.compressDirectory(outDir, OsPath.join(tmpDir, "output"), logDir, "tar.bz2");
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
		compressedDir = tfs.compressDirectory(outDir, OsPath.join(tmpDir, "output"), logDir, "tar");
		assertNotNull(compressedDir);
		assertTrue( OsPath.isfile(OsPath.join(tmpDir, "output.tar")) );
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
		compressedDir = tfs.compressDirectory(outDir, OsPath.join(tmpDir, "output"), logDir, "zip");
		assertNotNull(compressedDir);
		assertTrue( OsPath.isfile(OsPath.join(tmpDir, "output.zip")) );
		OsPath.delete(outDir);
		OsPath.mkdir(outDir);
		files = tfs.uncompressDirectory(compressedDir, outDir, logDir);
		assertEquals(2, files.size());
		Collections.sort(files);		
		assertTrue(fileCmp(files.get(0), OsPath.join(dataDir, "files/file1")));
		assertTrue(fileCmp(files.get(1), OsPath.join(dataDir, "files/file2")));	
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
	//@Test
	//public void testCompressDirectory4() throws IOException {
	//	String srcDir = OsPath.join(dataDir, "files");
	//	String compressedDir = tfs.compressDirectory(srcDir, tmpDir, "/foo", "tar.gz");
	//	assertNull(compressedDir);
	//}
	
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

	// Getter not tested
	//@Test
	//public void testGetValidCompression() {
	//	
	//}
	
}
