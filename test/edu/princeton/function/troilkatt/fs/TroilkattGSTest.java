package edu.princeton.function.troilkatt.fs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.*;
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

public class TroilkattGSTest extends TestSuper {		
	public TroilkattGS tfs;
	public TroilkattProperties troilkattProperties;	
	public ArrayList<String> localFiles;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		TestSuper.initTestDir();
		TestSuper.initTestDirGS();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {				
		troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));				
		tfs = new TroilkattGS(new Configuration());
		
		OsPath.deleteAll(tmpDir);
		OsPath.mkdir(tmpDir);
		OsPath.deleteAll(logDir);
		OsPath.mkdir(logDir);
		OsPath.deleteAll(outDir);
		OsPath.mkdir(outDir);
		OsPath.deleteAll(OsPath.join(nfsRoot, "out"));
		
		localFiles = new ArrayList<String>();
		localFiles.add(OsPath.join(dataDir, "files/file1"));
		localFiles.add(OsPath.join(dataDir, "files/file2"));
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testTroilkattGS() {				
		assertNotNull(tfs.logger);
	}

	
	@Test
	public void testListdir() throws IOException {		
		ArrayList<String> files = tfs.listdir(OsPath.join(nfsRoot, "ls")); //Fails
		
		System.out.println("TESTLISTDIR: " + OsPath.join(nfsRoot, "ls"));
		for(String fil : files) {
			System.out.println(fil);
		}

		assertEquals(6, files.size());
		Collections.sort(files);
		assertTrue(files.get(0).endsWith("file1"));
		assertTrue(files.get(1).endsWith("file2"));
		assertTrue(files.get(2).endsWith("file3"));		
	}
	
	// Empty directory
	@Test
	public void testListdir2() throws IOException {		
		ArrayList<String> files = tfs.listdir(OsPath.join(nfsRoot, "ls/subdir3")); //Fails
		assertEquals(0, files.size());		
	}
	
	// Invalid directory
	@Test
	public void testListdir3() throws IOException {			
		ArrayList<String> files = tfs.listdir(OsPath.join(nfsRoot, "ls/invalid-subdir")); //Fails
		//assertNull(files);
		assertEquals(0, files.size());
	}

	
	@Test
	public void testIsfile() throws IOException {
		assertTrue( tfs.isfile(OsPath.join(nfsRoot, "ls/file1")) );
		assertFalse( tfs.isfile(OsPath.join(nfsRoot, "ls")) ); //Fails
		assertFalse( tfs.isfile(OsPath.join(nfsRoot, "ls/non-existing")) );		
	}
	
	@Test
	public void testIsdir() throws IOException {
		//assertFalse( tfs.isdir(OsPath.join(nfsRoot, "ls/file1")) ); //Fails
		assertTrue( tfs.isdir(OsPath.join(nfsRoot, "ls")) );
		//assertFalse( tfs.isdir(OsPath.join(nfsRoot, "ls/non-existing")) );
	}

	@Test
	public void testMkdir() throws IOException {
		String hout = OsPath.join(nfsRoot, "out/test");
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
		String dstFile = OsPath.join(nfsRoot, "delete/file1");
		assertNotNull(dstFile);
		//OsPath.mkdir(OsPath.join(nfsRoot, "delete"));
		tfs.putLocalFile(OsPath.join(dataDir, "files/file1"), dstFile, tmpDir, logDir, "none");
		assertTrue(tfs.isfile(dstFile));
		assertTrue(tfs.deleteFile(dstFile));
		assertFalse(tfs.isfile(dstFile)); // Fails
		assertFalse(tfs.deleteFile(dstFile));
	}
	
	@Test(expected=RuntimeException.class)
	public void testDelete2() throws IOException {
		assertFalse(tfs.deleteFile(null)); // Fails
	}
	
	@Test
	public void testDeleteDir() throws IOException {
		String deleteDir = OsPath.join(nfsRoot, "delete");
		OsPath.mkdir(deleteDir);
		String dstFile1 = OsPath.join(nfsRoot, "delete/file1");
		String dstFile2 = OsPath.join(nfsRoot, "delete/file2");
		String dstFile3 = OsPath.join(nfsRoot, "delete/subdir/file3");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), dstFile1);
		OsPath.copy(OsPath.join(dataDir, "files/file2"), dstFile2);
		tfs.mkdir(OsPath.join(nfsRoot, "delete/subdir"));
		OsPath.copy(OsPath.join(dataDir, "files/file1"), dstFile3);		
		assertTrue(tfs.isfile(dstFile1));
		assertTrue(tfs.isfile(dstFile2));
		assertTrue(tfs.isfile(dstFile3));
		assertTrue(tfs.deleteDir(deleteDir));
		assertFalse(tfs.isfile(dstFile1)); //Fails
		assertFalse(tfs.isfile(dstFile2));
		assertFalse(tfs.isfile(dstFile3));
		
		assertFalse(tfs.deleteDir("/foo/bar"));
		
		OsPath.mkdir(deleteDir);
		OsPath.copy(OsPath.join(dataDir, "files/file1"), dstFile1);
		assertFalse(tfs.deleteDir(dstFile1));
		assertTrue(tfs.deleteDir(deleteDir));
	}

	@Test(expected=RuntimeException.class)
	public void testDeleteAll2() {
		assertFalse(OsPath.deleteAll(null));
	}
	
	@Test
	public void testGetFilenameDir() {
		assertEquals("/home/larsab/troilkatt/test/names", tfs.getFilenameDir("/home/larsab/troilkatt/test/names/foo.123.gz")); //Fails
		assertEquals("/home/larsab/troilkatt/test/names", tfs.getFilenameDir("/home/larsab/troilkatt/test/names/foo.123.gz"));
		assertEquals("troilkatt/test/names", tfs.getFilenameDir("troilkatt/test/names/foo.123"));		
	}
	
	@Test
	public void testListdirRecursive() throws IOException {		
		ArrayList<String> files = tfs.listdirR(OsPath.join(nfsRoot, "ls")); //Fails
		
		assertEquals(6, files.size());
		Collections.sort(files);
		assertTrue(files.get(0).endsWith("file1"));
		assertTrue(files.get(1).endsWith("file2"));
		assertTrue(files.get(2).endsWith("file3"));
		assertTrue(files.get(3).endsWith("subdir1/file4/file4"));
		assertTrue(files.get(4).endsWith("subdir1/file5/file5"));
		assertTrue(files.get(5).endsWith("subdir2/file6/file6"));
	}
	
	// Empty directory
	@Test
	public void testListdirRecursive2() throws IOException {
		ArrayList<String> files = tfs.listdirR(OsPath.join(nfsRoot, "ls/subdir3")); //Fails
		assertEquals(0, files.size());		
	}
	
	// Invalid directory
	@Test
	public void testListdirRecursive3() throws IOException {		
		ArrayList<String> files = tfs.listdirR(OsPath.join(nfsRoot, "ls/invalid-subdir")); //Fails
		//assertNull(files);		
		assertEquals(0, files.size());
	}
	
	@Test
	public void testListdirNewest() throws IOException {
		ArrayList<String> files = tfs.listdirN(OsPath.join(nfsRoot, "ts")); // Fails
		
		assertEquals(6, files.size());
		Collections.sort(files);
		assertTrue(files.get(0).endsWith("file1.3.gz"));
		assertTrue(files.get(1).endsWith("file2.3.bz2"));
		assertTrue(files.get(2).endsWith("file3.2.none"));
		assertTrue(files.get(3).endsWith("subdir1/file4.3.gz/file4.3.gz"));
		assertTrue(files.get(4).endsWith("subdir1/file5.2.none/file5.2.none"));
		assertTrue(files.get(5).endsWith("subdir2/file6.2.none/file6.2.none"));
	}
	
	// Empty directory
	@Test
	public void testListdirNewest2() throws IOException {		
		ArrayList<String> files = tfs.listdirN(OsPath.join(nfsRoot, "ts/subdir3")); // Fails
		assertEquals(0, files.size());		
	}
	
	// Invalid directory
	@Test
	public void testListdirNewest3() throws IOException {
		ArrayList<String> files = tfs.listdirN(OsPath.join(nfsRoot, "ts/invalid-subdir")); //Fails
		//assertNull(files);
		assertEquals(0, files.size());
	}
	
	@Test
	public void testGetNewestDir() throws IOException {
		String dir = tfs.getNewestDir(OsPath.join(nfsRoot, "tsd")); //Fails
		assertEquals("4.zip", dir);
	}

	// Invalid directory
	@Test
	public void testGetNewestDir2() throws IOException {
		String dir = tfs.getNewestDir(OsPath.join(nfsRoot, "tsd-invalid")); //Fails
		assertNull(dir);
	}
	
	// Empty directory
	@Test
	public void testGetNewestDir3() throws IOException {
		String dir = tfs.getNewestDir(OsPath.join(nfsRoot, "tsd-empty")); //Fails
		assertNull(dir);
	}
	
	@Test
	public void testListdirTimestamp() throws IOException {
		String cleanupRoot = OsPath.join(nfsRoot, "cleanup");
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
		
		ArrayList<String> files = tfs.listdirT(cleanupRoot, 1); //Fails
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
	
	private void initCleanupDir(String cleanupRoot, long ts) throws IOException {
		String testFile = OsPath.join(dataDir, "files/file1"); // on local FS
				
		tfs.mkdir(cleanupRoot);
		OsPath.copy(testFile, OsPath.join(cleanupRoot, "foo.1.gz"));
		OsPath.copy(testFile, OsPath.join(cleanupRoot, "bar.2.gz"));
		OsPath.copy(testFile, OsPath.join(cleanupRoot, "foo." + ts + ".gz"));
		OsPath.copy(testFile, OsPath.join(cleanupRoot, "baz." + ts + ".gz"));
		OsPath.copy(testFile, OsPath.join(cleanupRoot, "bongo." + ts + ".gz"));
		
		tfs.mkdir(OsPath.join(cleanupRoot, "subdir"));
		OsPath.copy(testFile, OsPath.join(cleanupRoot, "subdir/sd-foo.1.gz"));
		OsPath.copy(testFile, OsPath.join(cleanupRoot, "subdir/sd-bar.2.gz"));
		OsPath.copy(testFile, OsPath.join(cleanupRoot, "subdir/sd-foo." + ts + ".gz"));
	}
	
	@Test
	public void testCleanupDir() throws IOException {
		String cleanupRoot = OsPath.join(nfsRoot, "cleanup");          // on HDFS
		long ts = 15 * 1000 * 60 * 60 * 24; // 15 days in ms
		initCleanupDir(cleanupRoot, ts);
		
		long cts = 20 * 1000 * 60 * 60 * 24; // 20 days in ms
		tfs.cleanupDir(cleanupRoot, cts, 10); //Fails
		
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
	
	@Test
	public void testCleanupMetaDir() throws IOException {	
		String cleanupRoot = OsPath.join(nfsRoot, "cleanup");          // on HDFS
		String testFile = OsPath.join(dataDir, "files/file1"); // on local FS
		
		long msPerDay = 1000 * 60 * 60 * 24;
		long ts1 = 15 * msPerDay; // 15 days in ms
		long ts2 = 20 * msPerDay; // 20 days in ms
		long ts3 = 25 * msPerDay; // 25 days in ms
		
		tfs.deleteDir(cleanupRoot);
		assertFalse(tfs.isdir(cleanupRoot)); //Fails
		OsPath.mkdir(cleanupRoot);
		OsPath.copy(testFile, OsPath.join(cleanupRoot, "1.gz"));
		OsPath.copy(testFile, OsPath.join(cleanupRoot, "2.gz"));
		OsPath.copy(testFile, OsPath.join(cleanupRoot, ts1 + ".gz"));
		OsPath.copy(testFile, OsPath.join(cleanupRoot, ts2 + ".gz"));
				
		tfs.cleanupMetaDir(cleanupRoot, ts3, 10);
		
		ArrayList<String> files = tfs.listdirR(cleanupRoot);
		Collections.sort(files);
		assertEquals(2, files.size());
		assertTrue(files.get(0).endsWith(ts1 + ".gz"));
		assertTrue(files.get(1).endsWith(ts2 + ".gz"));		
		
		tfs.cleanupMetaDir(cleanupRoot, ts3, 30);
		files = tfs.listdirR(cleanupRoot);
		Collections.sort(files);
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
		
		OsPath.copy(testFile, OsPath.join(cleanupRoot, "1.gz"));
		OsPath.copy(testFile, OsPath.join(cleanupRoot, "2.gz"));
		OsPath.copy(testFile, OsPath.join(cleanupRoot, ts1 + ".gz"));
		OsPath.copy(testFile, OsPath.join(cleanupRoot, ts2 + ".gz"));
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
	public void testGetFile() throws IOException {	
		String file = tfs.getFile(OsPath.join(nfsRoot, "compressed-files/file1.1.gz"), //Fails
				outDir, tmpDir, logDir);
		assertTrue(file.endsWith(OsPath.join(outDir, "file1")));
		assertTrue(fileCmp(file, OsPath.join(dataDir, "files/file1")));
	}
	
	@Test
	public void testGetFile2() throws IOException {
		String file = tfs.getFile(OsPath.join(nfsRoot, "compressed-files/file1.2.bz2"), //Fails
				outDir, tmpDir, logDir);
		assertTrue(file.endsWith(OsPath.join(outDir, "file1")));
		assertTrue(fileCmp(file, OsPath.join(dataDir, "files/file1")));
	}
	
	@Test
	public void testGetFile3() throws IOException {
		// Zip is not supported
		String file = tfs.getFile(OsPath.join(nfsRoot, "compressed-files/file1.3.zip"), //Fails
				outDir, tmpDir, logDir);
		assertNull(file);
		//assertTrue(file.endsWith(OsPath.join(outDir, "file1")));
		//assertTrue(fileCmp(file, OsPath.join(dataDir, "files/file1")));
	}
	
	@Test
	public void testGetFile4() throws IOException {		
		String file = tfs.getFile(OsPath.join(nfsRoot, "compressed-files/file1.4.none"), //Fails
				outDir, tmpDir, logDir);
		assertTrue(file.endsWith(OsPath.join(outDir, "file1")));
		assertTrue(fileCmp(file, OsPath.join(dataDir, "files/file1")));
	}
	
	// Invalid filename
	@Test
	public void testGetFile5() throws IOException {		
		String file = tfs.getFile(OsPath.join(nfsRoot, "ls/file1"), //Fails
				outDir, tmpDir, logDir);
		assertNull(file);
	}
	
	// Invalid file
	@Test
	public void testGetFile6() throws IOException {
		String file = tfs.getFile(OsPath.join(nfsRoot, "compressed-files/invalid.5.gz"), //Fails
				outDir, tmpDir, logDir);
		assertNull(file);
	}
	
	// Invalid local dir
	@Test
	public void testGetFile7() throws IOException {		
		String file = tfs.getFile(OsPath.join(nfsRoot, "compressed-files/file1.1.gz"), //Fails
				"/foo/bar", tmpDir, logDir);
		assertNull(file);
	}
	
	// Invalid log dir
	//@Test
	public void testGetFile8() throws IOException {
		String file = tfs.getFile(OsPath.join(nfsRoot, "compressed-files/file1.3.zip"),
				outDir, tmpDir, "/foo/bar");
		assertNull(file);
	}
	
	// Invalid tmp dir
	@Test
	public void testGetFile9() throws IOException {
		String file = tfs.getFile(OsPath.join(nfsRoot, "compressed-files/file1.3.zip"), //Fails
				outDir, "/foo/bar", logDir);
		assertNull(file);
	}

	@Test
	public void testGetDirFiles() throws IOException {
		ArrayList<String> files = tfs.getDirFiles(OsPath.join(nfsRoot, "compressed-dirs/1.tar.gz"),
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
		ArrayList<String> files = tfs.getDirFiles(OsPath.join(nfsRoot, "compressed-dirs/2.tar.bz2"),
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
		ArrayList<String> files = tfs.getDirFiles(OsPath.join(nfsRoot, "compressed-dirs/3.zip"),
				outDir, logDir, tmpDir);
		assertEquals(files.size(), 2);
		Collections.sort(files);
		assertTrue(files.get(0).endsWith(OsPath.join(outDir, "files/file1")));
		assertTrue(files.get(1).endsWith(OsPath.join(outDir, "files/file2")));
		assertTrue(fileCmp(files.get(0), OsPath.join(dataDir, "files/file1")));
		assertTrue(fileCmp(files.get(1), OsPath.join(dataDir, "files/file2")));
	}

	@Test
	public void testGetDirFiles4() throws IOException {		
		ArrayList<String> files = tfs.getDirFiles(OsPath.join(nfsRoot, "compressed-dirs/4.none"),
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
		ArrayList<String> files = tfs.getDirFiles(OsPath.join(nfsRoot, "compressed-dirs/3242.none"),
				outDir, logDir, tmpDir);
		assertNull(files);		
	}

	// Invalid file
	@Test
	public void testGetDirFiles6() throws IOException {		
		ArrayList<String> files = tfs.getDirFiles(OsPath.join(nfsRoot, "compressed-dirs/invalid.zip"),
				outDir, logDir, tmpDir);
		assertNull(files);		
	}

	// Invalid local dir
	@Test(expected=IOException.class)
	public void testGetDirFiles7() throws IOException {
		ArrayList<String> files = tfs.getDirFiles(OsPath.join(nfsRoot, "compressed-dirs/2.tar.bz2"),
				"/foo/bar", logDir, tmpDir);
		assertNull(files);		
	}

	// Invalid log dir
	//@Test
	//public void testGetDirFiles8() throws IOException {
	//	ArrayList<String> files = tfs.getDirFiles(OsPath.join(nfsRoot, "compressed-dirs/2.tar.bz2"),
	//			outDir, "/foo/bar", tmpDir);
	//	assertNull(files);		
	//}

	// Invalid tmp dir
	// ERROR: tmp dir is not used in getDirFiles!
	//@Test
	//public void testGetDirFiles9() throws IOException {		
	//	ArrayList<String> files = tfs.getDirFiles(OsPath.join(nfsRoot, "compressed-dirs/2.tar.bz2"),
	//			outDir, tmpDir, "/foo/bar");
	//	assertNull(files);		
	//}

	@Test
	public void testPutFile() throws IOException {
		String houtDir = OsPath.join(nfsRoot, "out");
		OsPath.mkdir(houtDir);
		
		String srcFile = OsPath.join(tmpDir, "file1");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);		
		String nfsName = tfs.putLocalFile(srcFile, houtDir, tmpDir, logDir, "gz", 1);
		assertEquals(OsPath.join(houtDir, "file1.1.gz"), nfsName);
		String localName = tfs.getFile(nfsName, outDir, tmpDir, logDir);
		assertNotNull(localName);
		assertTrue(localName.endsWith(OsPath.join(outDir, "file1")));
		assertTrue(fileCmp(localName, OsPath.join(dataDir, "files/file1")));
		
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);
		nfsName = tfs.putLocalFile(srcFile, houtDir, tmpDir, logDir, "bz2", 2);
		assertEquals(OsPath.join(houtDir, "file1.2.bz2"), nfsName);
		localName = tfs.getFile(nfsName, outDir, tmpDir, logDir);
		assertTrue(localName.endsWith(OsPath.join(outDir, "file1")));
		assertTrue(fileCmp(localName, OsPath.join(dataDir, "files/file1")));
		
		// Zip compression is not supported
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);
		nfsName = tfs.putLocalFile(srcFile, houtDir, tmpDir, logDir, "zip", 3);
		assertNull(nfsName);
		//assertEquals(OsPath.join(houtDir, "file1.3.zip"), nfsName);
		//localName = tfs.getFile(nfsName, outDir, tmpDir, logDir);
		//assertTrue(localName.endsWith(OsPath.join(outDir, "file1")));
		//assertTrue(fileCmp(localName, OsPath.join(dataDir, "files/file1")));
		
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);
		nfsName = tfs.putLocalFile(srcFile, houtDir, tmpDir, logDir, "none", 4);
		assertEquals(OsPath.join(houtDir, "file1.4.none"), nfsName);
		localName = tfs.getFile(nfsName, outDir, tmpDir, logDir); 
		assertTrue(localName.endsWith(OsPath.join(outDir, "file1")));
		assertTrue(fileCmp(localName, OsPath.join(dataDir, "files/file1")));		
	}
	
	// Invalid local filename
	@Test
	public void testPutFile2() throws IOException {
		String houtDir = OsPath.join(nfsRoot, "out");
		OsPath.mkdir(houtDir);
		
		String nfsName = tfs.putLocalFile(OsPath.join(dataDir, "files/non-existing"), houtDir, tmpDir, logDir, "gz", 1);
		assertNull(nfsName);
	}
	
	// Invalid HDFS dir filename
	// Permissions do not work correctly in pseudo mode
	//@Test
	//public void testPutFile3() throws IOException {
	//	String srcFile = OsPath.join(tmpDir, "file1");
	//	OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);		
	//	String nfsName = tfs.putFile(srcFile, "/foo/bar", tmpDir, logDir, "gz", 1);
	//	assertNull(nfsName);
	//}
	
	// Invalid tmp dir
	@Test
	public void testPutFile4() throws IOException {
		String houtDir = OsPath.join(nfsRoot, "out");
		OsPath.mkdir(houtDir);
		
		String srcFile = OsPath.join(tmpDir, "file1");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);		
		String nfsName = tfs.putLocalFile(srcFile, houtDir, "/foo", logDir, "zip", 1);
		assertNull(nfsName);
	}
	
	// Invalid compresison
	@Test
	public void testPutFile5() throws IOException {
		String houtDir = OsPath.join(nfsRoot, "out");
		OsPath.mkdir(houtDir);
		
		String srcFile = OsPath.join(tmpDir, "file1");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);		
		String nfsName = tfs.putLocalFile(srcFile, houtDir, tmpDir, logDir, "foo", 1);
		assertNull(nfsName);
	}
	
	// Invalid timestamp
	@Test
	public void testPutFile6() throws IOException {
		String houtDir = OsPath.join(nfsRoot, "out");
		OsPath.mkdir(houtDir);
		
		String srcFile = OsPath.join(tmpDir, "file1");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);		
		String nfsName = tfs.putLocalFile(srcFile, houtDir, tmpDir, logDir, "gz", -321);
		assertNull(nfsName);
	}
	
	// Invalid log dir
	@Test
	public void testPutFile7() throws IOException {
		String houtDir = OsPath.join(nfsRoot, "out");
		OsPath.mkdir(houtDir);
		
		String srcFile = OsPath.join(tmpDir, "file1");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);		
		String nfsName = tfs.putLocalFile(srcFile, houtDir, tmpDir, "/foo/bar", "zip", 1);
		assertNull(nfsName);
	}
	
	@Test
	public void testPutFile8() throws IOException {
		String houtDir = OsPath.join(nfsRoot, "out");
		OsPath.mkdir(houtDir);
		
		String srcFile = OsPath.join(tmpDir, "file1");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);		
		String nfsName = tfs.putLocalFile(srcFile, houtDir, tmpDir, logDir, "gz");
		assertEquals(OsPath.join(houtDir, "file1.gz"), nfsName);
		
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);
		nfsName = tfs.putLocalFile(srcFile, houtDir, tmpDir, logDir, "bz2");
		assertEquals(OsPath.join(houtDir, "file1.bz2"), nfsName);
		
		// Zip compression is not supported
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);
		nfsName = tfs.putLocalFile(srcFile, houtDir, tmpDir, logDir, "zip");
		assertNull(nfsName);
		//assertEquals(OsPath.join(houtDir, "file1.zip"), nfsName);
		
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);
		nfsName = tfs.putLocalFile(srcFile, houtDir, tmpDir, logDir, "none");
		assertEquals(OsPath.join(houtDir, "file1.none"), nfsName);
	}

	// Invalid local filename
	@Test
	public void testPutFile9() throws IOException {
		String houtDir = OsPath.join(nfsRoot, "out");
		OsPath.mkdir(houtDir);
		
		String nfsName = tfs.putLocalFile(OsPath.join(dataDir, "files/non-existing"), houtDir, tmpDir, logDir, "gz");
		assertNull(nfsName);
	}
	
	// Invalid HDFS dir filename
	// Permissions do not work correctly in pseudo mode
	//@Test
	//public void testPutFile10() throws IOException {
	//	String srcFile = OsPath.join(tmpDir, "file1");
	//	OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);		
	//	String nfsName = tfs.putFile(srcFile, "/foo/bar", tmpDir, logDir, "gz");
	//	assertNull(nfsName);
	//}
	
	// Invalid tmp dir
	@Test
	public void testPutFile11() throws IOException {
		String houtDir = OsPath.join(nfsRoot, "out");
		OsPath.mkdir(houtDir);
		
		String srcFile = OsPath.join(tmpDir, "file1");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);		
		String nfsName = tfs.putLocalFile(srcFile, houtDir, "/foo", logDir, "zip");
		assertNull(nfsName);
	}
	
	// Invalid log dir
	@Test
	public void testPutFile12() throws IOException {
		String houtDir = OsPath.join(nfsRoot, "out");
		OsPath.mkdir(houtDir);
		
		String srcFile = OsPath.join(tmpDir, "file1");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);		
		String nfsName = tfs.putLocalFile(srcFile, houtDir, tmpDir, "/foo/bar", "zip");
		assertNull(nfsName);
	}
	
	// Invalid compresison
	@Test
	public void testPutFile13() throws IOException {
		String houtDir = OsPath.join(nfsRoot, "out");
		OsPath.mkdir(houtDir);
		
		String srcFile = OsPath.join(tmpDir, "file1");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);		
		String nfsName = tfs.putLocalFile(srcFile, houtDir, tmpDir, logDir, "foo");
		assertNull(nfsName);
	}
	
	// Empty file
	@Test
	public void testPutFile14() throws IOException {
		String houtDir = OsPath.join(nfsRoot, "out");
		OsPath.mkdir(houtDir);
		
		String srcFile = OsPath.join(tmpDir, "empty");
		byte[] empty = new byte[0];
		FSUtils.writeFile(srcFile, empty);
		
		String nfsName = tfs.putLocalFile(srcFile, houtDir, tmpDir, logDir, "foo");
		assertNull(nfsName);
	}
	
	@Test
	public void testPutDirFiles() throws IOException {
		String houtDir = OsPath.join(nfsRoot, "out");
		OsPath.mkdir(houtDir);
		
		// Zip compression is not supported
		//assertFalse( tfs.putLocalDirFiles(houtDir, 10, localFiles, "zip", logDir, tmpDir) );
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
		assertFalse( tfs.putLocalDirFiles("/foo/bar", 10, localFiles, "tar", logDir, tmpDir) );
	}
	
	// Invalid timestamp
	@Test
	public void testPutDirFiles3() throws IOException {
		String houtDir = OsPath.join(nfsRoot, "out");
		OsPath.mkdir(houtDir);
		
		assertFalse( tfs.putLocalDirFiles(houtDir, -10, localFiles, "tar", logDir, tmpDir) );
	}
	
	// Invalid local file
	@Test
	public void testPutDirFiles4() throws IOException {
		String houtDir = OsPath.join(nfsRoot, "out");
		OsPath.mkdir(houtDir);
		localFiles.add("/foo/bar/baz");
		assertFalse( tfs.putLocalDirFiles(houtDir, 10, localFiles, "tar", logDir, tmpDir) );
	}
	
	// No local files
	@Test
	public void testPutDirFiles5() throws IOException {
		String houtDir = OsPath.join(nfsRoot, "out");
		OsPath.mkdir(houtDir);
		localFiles.clear();
		// not an error
		assertTrue( tfs.putLocalDirFiles(houtDir, 10, localFiles, "tar", logDir, tmpDir) );
	}
	
	// Invalid compression
	@Test
	public void testPutDirFiles6() throws IOException {
		String houtDir = OsPath.join(nfsRoot, "out");
		OsPath.mkdir(houtDir);
		
		assertFalse( tfs.putLocalDirFiles(houtDir, 10, localFiles, "foo", logDir, tmpDir) );
	}
	
	// Invalid log dir
	//@Test
	//public void testPutDirFiles7() throws IOException {
	//	String houtDir = OsPath.join(nfsRoot, "out");
	//	OsPath.mkdir(houtDir));
	//	
	//	assertFalse( tfs.putLocalDirFiles(houtDir, 10, localFiles, "tar", "/doo/bar", tmpDir) );
	//}
	
	// Invalid tmpDir
	@Test
	public void testPutDirFiles8() throws IOException {
		String houtDir = OsPath.join(nfsRoot, "out");
		OsPath.mkdir(houtDir);
		
		assertFalse( tfs.putLocalDirFiles(houtDir, 10, localFiles, "tar", logDir, "/doo/bna") );
	}

	@Test
	public void testMoveFile() throws IOException {
		String srcFile = OsPath.join(nfsRoot, "moveSrc/file1");
		OsPath.mkdir(OsPath.join(nfsRoot, "moveSrc"));
		String dstDir = OsPath.join(nfsRoot, "moveDst");
		String dstFile = OsPath.join(nfsRoot, "moveDst/file1.4.none");
		
		/*
		 * Prepare
		 */
		tfs.deleteDir(dstDir);
		tfs.mkdir(dstDir);
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);		
		assertTrue(tfs.isfile(srcFile));
		assertFalse(tfs.isfile(dstFile));				 
		assertEquals(dstFile, tfs.putTFSFile(srcFile, dstDir, tmpDir, logDir, "none", 4));		
		assertFalse(tfs.isfile(srcFile));
		assertTrue(tfs.isfile(dstFile));
		
		dstFile = OsPath.join(nfsRoot, "moveDst/file1.5.gz");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);		
		assertTrue(tfs.isfile(srcFile));
		assertFalse(tfs.isfile(dstFile));				 
		assertEquals(dstFile, tfs.putTFSFile(srcFile, dstDir, tmpDir, logDir, "gz", 5));		
		assertFalse(tfs.isfile(srcFile));
		assertTrue(tfs.isfile(dstFile));
		
		dstFile = OsPath.join(nfsRoot, "moveDst/file1.6.bz2");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);		
		assertTrue(tfs.isfile(srcFile));
		assertFalse(tfs.isfile(dstFile));				 
		assertEquals(dstFile, tfs.putTFSFile(srcFile, dstDir, tmpDir, logDir, "bz2", 6));		
		assertFalse(tfs.isfile(srcFile));
		assertTrue(tfs.isfile(dstFile));
		
		srcFile = OsPath.join(nfsRoot, "moveSrc/file1");
		// Move back to source directory
		assertTrue(OsPath.rename(OsPath.join(nfsRoot, "moveDst/file1.5.gz"), srcFile));		
		dstFile = OsPath.join(nfsRoot, "moveDst/file1.7.gz");				
		assertTrue(tfs.isfile(srcFile));
		assertFalse(tfs.isfile(dstFile));				 
		assertEquals(dstFile, tfs.putTFSFile(srcFile, dstDir, tmpDir, logDir, "gz", 7));		
		assertFalse(tfs.isfile(srcFile));
		assertTrue(tfs.isfile(dstFile));
		
		// Move back to source directory
		assertTrue(OsPath.rename(dstFile, srcFile));		
		dstFile = OsPath.join(nfsRoot, "moveDst/subdir/file1.9.gz");				
		assertTrue(tfs.isfile(srcFile));
		assertFalse(tfs.isfile(dstFile));				 
		assertEquals(dstFile, tfs.putTFSFile(srcFile, OsPath.join(dstDir, "subdir"),
				tmpDir, logDir, "gz", 9));		
		assertFalse(tfs.isfile(srcFile));
		assertTrue(tfs.isfile(dstFile));
				
		// Move back to source directory
		assertTrue(OsPath.rename(dstFile, srcFile));		
		dstFile = OsPath.join(nfsRoot, "moveDst/file1.8.bz2");				
		assertTrue(tfs.isfile(srcFile));
		assertFalse(tfs.isfile(dstFile));				 
		assertEquals(dstFile, tfs.putTFSFile(srcFile, dstDir, tmpDir, logDir, "bz2", 8));		
		assertFalse(tfs.isfile(srcFile));
		assertTrue(tfs.isfile(dstFile));
		
		// Overwrite file
		srcFile = OsPath.join(nfsRoot, "moveSrc/file1");		
		dstFile = OsPath.join(nfsRoot, "moveDst/file1.10.none");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);		
		OsPath.copy(OsPath.join(dataDir, "files/file1"), dstFile);		
		assertTrue(tfs.isfile(srcFile));
		assertTrue(tfs.isfile(dstFile));
		assertEquals(dstFile, tfs.putTFSFile(srcFile, dstDir, tmpDir, logDir, "none", 10));
		assertFalse(tfs.isfile(srcFile));
		assertTrue(tfs.isfile(dstFile));
		
		// Invalid source file
		srcFile = "/invalid/dir/filename";
		dstFile = OsPath.join(nfsRoot, "moveDst/file1.9.gz");				
		assertFalse(tfs.isfile(srcFile));
		assertFalse(tfs.isfile(dstFile));				 
		assertNull(tfs.putTFSFile(srcFile, dstDir, tmpDir, logDir, "gz", 9));				
		assertFalse(tfs.isfile(dstFile));

		srcFile = OsPath.join(nfsRoot, "moveSrc/file1");
		dstFile = OsPath.join(nfsRoot, "moveDst/file1.9.gz");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);
				
		// Invalid compression format
		assertTrue(tfs.isfile(srcFile));
		assertFalse(tfs.isfile(dstFile));
		assertNull(tfs.putTFSFile(srcFile, dstDir, tmpDir, logDir, "invalid", 10));
		assertTrue(tfs.isfile(srcFile));
		
		// Invalid timestamp
		assertTrue(tfs.isfile(srcFile));
		assertFalse(tfs.isfile(dstFile));
		assertNull(tfs.putTFSFile(srcFile, dstDir, tmpDir, logDir, "bz2", -10));
		assertTrue(tfs.isfile(srcFile));				
	}
	
	/* Invalid destination directory
	 */
	@Test 
	public void testMoveFileI1() throws IOException {
		String srcFile = OsPath.join(nfsRoot, "moveSrc/file1");
		String dstFile = OsPath.join(nfsRoot, "moveDst/file1.9.gz");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);			
		assertTrue(tfs.isfile(srcFile));
		assertFalse(tfs.isfile(dstFile));
		assertNull(tfs.putTFSFile(srcFile, "/foo/bar/baz", tmpDir, logDir, "gz", 9));
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
	//	OsPath.copy(false, true,
	//			OsPath.join(dataDir, "files/file1")),
	//			srcFile));			
	//	String dstDir = OsPath.join(testRoot, "moveDst");
	//	String dstFile = OsPath.join(testRoot, "moveDst/file1.10.bz2");
	//	assertTrue(tfs.isfile(srcFile));
	//	assertFalse(tfs.isfile(dstFile));
	//	tfs.putTFSFile(srcFile, dstDir, "/foo/bar", logDir, "bz2", 10);
	//	assertTrue(tfs.isfile(srcFile));
	//}
	
	// Invalid log directory
	//@Test(expected=IOException.class) 
	//public void testMoveFileI3() throws IOException {
	//	String srcFile = OsPath.join(testRoot, "moveSrc/file1");
	//	OsPath.copy(false, true,
	//			OsPath.join(dataDir, "files/file1")),
	//			srcFile));			
	//	String dstDir = OsPath.join(testRoot, "moveDst");
	//	String dstFile = OsPath.join(testRoot, "moveDst/file1.11.bz2");
	//	assertTrue(tfs.isfile(srcFile));
	//	assertFalse(tfs.isfile(dstFile));
	//	tfs.putTFSFile(srcFile, dstDir, tmpDir, "/foo/bar", "bz2", 10);
	//	assertTrue(tfs.isfile(srcFile));
	//}
	
	
	@Test
	public void testMoveFile2() throws IOException {
		String localFile1 = OsPath.join(dataDir, "files/file1");
		String localFile2 = OsPath.join(tmpDir, "file1");
		String srcDir = OsPath.join(nfsRoot, "moveSrc");
		String dstDir = OsPath.join(nfsRoot, "moveDst");		
		
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
		assertEquals(dstFile, tfs.putTFSFile(srcFile, dstDir));
		assertFalse(tfs.isfile(srcFile));
		assertTrue(tfs.isfile(dstFile));
		
		srcFile = OsPath.join(srcDir, "file1.5.gz");
		dstFile = OsPath.join(dstDir, "file1.5.gz");
		assertTrue(tfs.isfile(srcFile));
		assertFalse(tfs.isfile(dstFile));
		assertEquals(dstFile, tfs.putTFSFile(srcFile, dstDir));
		assertFalse(tfs.isfile(srcFile));
		assertTrue(tfs.isfile(dstFile));
		
		srcFile = OsPath.join(srcDir, "file1.6.bz2");
		dstFile = OsPath.join(dstDir, "file1.6.bz2");
		assertTrue(tfs.isfile(srcFile));
		assertFalse(tfs.isfile(dstFile));
		assertEquals(dstFile, tfs.putTFSFile(srcFile, dstDir));
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
		assertEquals(dstFile, tfs.putTFSFile(srcFile, OsPath.join(dstDir, "subdir")));
		assertFalse(tfs.isfile(srcFile));
		assertTrue(tfs.isfile(dstFile));
		
		// Invalid source file
		assertNull(tfs.putTFSFile("/invalud/file", dstDir));
		
		// Missing timestamp
		srcFile = OsPath.join(dstDir, "file1.gz");
		OsPath.copy(localFile1, localFile2);
		OsPath.rename(localFile2, srcFile);
		assertTrue(tfs.isfile(srcFile));
		assertNull(tfs.putTFSFile(srcFile, dstDir));
		
		// Missing compression
		srcFile = OsPath.join(dstDir, "file1.5");
		OsPath.copy(localFile1, localFile2);
		OsPath.rename(localFile2, srcFile);
		assertTrue(tfs.isfile(srcFile));
		assertNull(tfs.putTFSFile(srcFile, dstDir));
		
		// Missing timestamp and compression		
		srcFile = OsPath.join(dstDir, "file1");
		OsPath.copy(localFile1, localFile2);
		OsPath.rename(localFile2, srcFile);
		assertTrue(tfs.isfile(srcFile));
		assertNull(tfs.putTFSFile(srcFile, dstDir));
		
		// Invalid destination directory
		// Missing timestamp
		srcFile = OsPath.join(dstDir, "file1.8.gz");
		OsPath.copy(localFile1, localFile2);
		OsPath.rename(localFile2, srcFile);
		assertTrue(tfs.isfile(srcFile));
		// Note fails since pseudo mode does not respect directory user flags
		//assertNull(tfs.putTFSFile(srcFile, "/invalid/dir"));
	}
	
	@Test
	public void testRenameFile() throws IOException {
		String srcFile = OsPath.join(nfsRoot, "moveSrc/file1");
		String dstDir = OsPath.join(nfsRoot, "moveDst");
		String dstFile = OsPath.join(nfsRoot, "moveDst/file1");
		
		/*
		 * Prepare
		 */
		tfs.deleteDir(dstDir);
		tfs.mkdir(dstDir);
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);		
		assertTrue(tfs.isfile(srcFile));
		assertFalse(tfs.isfile(dstFile));				 
		
		
		// Valid move
		assertTrue(tfs.renameFile(srcFile, dstFile));
		assertFalse(tfs.isfile(srcFile));
		assertTrue(tfs.isfile(dstFile));
		
		// Also valid, even if dest already exists
		OsPath.copy(OsPath.join(dataDir, "files/file1"), srcFile);
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
		String filename = OsPath.join(nfsRoot, "ls/file1");		
		OsPath.copy(OsPath.join(dataDir, "files/file1"), filename);		
		assertTrue(tfs.isfile(filename));
				
		assertEquals(8396983, tfs.fileSize(filename));
		
		String srcFile = OsPath.join(tmpDir, "empty");
		byte[] empty = new byte[0];
		FSUtils.writeFile(srcFile, empty);
		tfs.mkdir(OsPath.join(nfsRoot, "tmp"));
		OsPath.copy(srcFile, OsPath.join(nfsRoot, "tmp/empty"));		
		assertEquals(0,tfs.fileSize( OsPath.join(nfsRoot, "tmp/empty")));
	}
}