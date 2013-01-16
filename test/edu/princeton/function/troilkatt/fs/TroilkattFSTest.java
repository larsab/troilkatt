package edu.princeton.function.troilkatt.fs;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
		tfs = new TroilkattFS();
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
}
