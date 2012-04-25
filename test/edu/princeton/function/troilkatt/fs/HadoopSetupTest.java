package edu.princeton.function.troilkatt.fs;


import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import edu.princeton.function.troilkatt.TestSuper;


/**
 * A few tests to make sure that hadoop configuration is correctly set up.
 * 
 * Note! exceptions and invalid arguments are NOT tested.
 */
public class HadoopSetupTest extends TestSuper {
	protected String testDir = "troilkatt/test/setup";
	protected String[] dirContent = {"foo", "bar", "baz"};
	protected String newLocalFSFile = "test-data/test.file";
	protected String newLocalFSFile2 = "test-data/test.file2";
	protected String newHDFSFile = OsPath.join(testDir, "test.file");

	
	@Test
	public void testConnect() throws IOException {
		Configuration hdfsConfig = new Configuration();
		FileSystem hdfs = FileSystem.get(hdfsConfig);
		hdfs.close();
	}
	
	@Test
	public void testPut() throws IOException {
		Configuration hdfsConfig = new Configuration();
		FileSystem hdfs = FileSystem.get(hdfsConfig);
		
		Path localSrcPath = new Path(newLocalFSFile);		
		
		Path hdfsPath = new Path(OsPath.join(testDir, "foo"));		
		hdfs.copyFromLocalFile(false, true, localSrcPath, hdfsPath); // keep local & overwrite		
		hdfsPath = new Path(OsPath.join(testDir, "bar"));		
		hdfs.copyFromLocalFile(false, true, localSrcPath, hdfsPath); // keep local & overwrite
		hdfsPath = new Path(OsPath.join(testDir, "baz"));		
		hdfs.copyFromLocalFile(false, true, localSrcPath, hdfsPath); // keep local & overwrite
		
		hdfs.close();
	}
	
	@Test
	public void testDirlist() throws IOException {
		Configuration hdfsConfig = new Configuration();
		FileSystem hdfs = FileSystem.get(hdfsConfig);
		
		FileStatus[] files = hdfs.listStatus(new Path(testDir));
		assertEquals(files.length, dirContent.length);
		
		String[] filenames = new String[files.length];
		for (int i = 0; i < files.length; i++) {
			filenames[i] = OsPath.basename(files[i].getPath().toString());
		}
		
		for (String f: filenames) {
			boolean isFound = false;
			for (String g: dirContent) {
				if (g.equals(f)) {
					isFound = true;
				}
			}
			assertTrue(isFound);
		}
		
		for (String g: dirContent) {
			boolean isFound = false;
			for (String f: filenames) {
				if (g.equals(f)) {
					isFound = true;
				}
			}
			assertTrue(isFound);
		}
		
		hdfs.close();
	}
	
	@Test
	public void testPutGetDelete() throws IOException {
		Configuration hdfsConfig = new Configuration();
		FileSystem hdfs = FileSystem.get(hdfsConfig);
		
		Path localSrcPath = new Path(newLocalFSFile);
		Path localDstPath = new Path(newLocalFSFile2);
		Path hdfsPath = new Path(newHDFSFile);
		
		// Test put
		assertFalse(hdfs.isFile(hdfsPath));
		hdfs.copyFromLocalFile(false, true, localSrcPath, hdfsPath); // keep local & overwrite
		assertTrue(hdfs.isFile(hdfsPath));
		
		// Test get
		OsPath.delete(newLocalFSFile2);
		hdfs.copyToLocalFile(hdfsPath, localDstPath);		
		assertTrue(fileCmp(newLocalFSFile, newLocalFSFile2));
		
		// Test delete
		hdfs.delete(hdfsPath, false);
		assertFalse(hdfs.isFile(hdfsPath));
		
		hdfs.close();
	}
}
