package edu.princeton.function.troilkatt;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.OsPath;

public class TroilkattStatusTest extends TestSuper {
	protected Troilkatt troilkatt;
	protected TroilkattProperties troilkattProperties;
	protected String statusFilename = null;	
	protected Path hdfsStatusPath = null;
	protected Path statusPath = null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		troilkatt = new Troilkatt();
		troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));	
		if (troilkattProperties == null) {
			throw new Exception("Could not initialize troilkatt properties");
		}
		String troilkattDir = troilkattProperties.get("troilkatt.localfs.dir");		
		hdfsStatusPath = new Path(troilkattProperties.get("troilkatt.hdfs.status.file"));
		statusFilename = OsPath.join(troilkattDir, hdfsStatusPath.getName());		
		statusPath = new Path(statusFilename);
		
		createStatusFile(statusFilename);		
		System.out.printf("Copy %s to %s\n", statusFilename, troilkattProperties.get("troilkatt.hdfs.status.file"));
		OsPath.delete(OsPath.join(troilkattDir, ".status-test.txt.crc"));
		troilkatt.tfs.hdfs.copyFromLocalFile(false, true, statusPath, hdfsStatusPath); // keep local & overwrite
	}

	private void createStatusFile(String statusFilename2) throws IOException {
		File file = new File(statusFilename2);
		BufferedWriter os = new BufferedWriter(new FileWriter(file));
		for (int i = 0; i < 4; i++) { 
			os.write(i + ":Troilkatt:start\n");
			os.write(i + ":theSource:start\n");
			os.write(i + ":theSource:done\n");
			os.write(i + ":firstStage:start\n");
			os.write(i + ":firstStage:done\n");
			os.write(i + ":secondStage:start\n");
			os.write(i + ":secondStage:done\n");
			os.write(i + ":thirdStage:start\n");
			os.write(i + ":thirdStage:done\n");
			os.write(i + ":theSink:start\n");
			os.write(i + ":theSink:done\n");
			os.write(i + ":Troilkatt:done\n");
		}
		
		os.write("4:Troilkatt:start\n");
		os.write("4:theSource:start\n");
		os.write("4:theSource:done\n");
		os.write("4:firstStage:start\n");
		os.write("4:firstStage:done\n");
		os.write("4:secondStage:recover\n");
		
		os.close();
	}

	@After
	public void tearDown() throws Exception {
		OsPath.delete(statusFilename + ".2");
		OsPath.delete(statusFilename + ".modified");
	}

	@Test
	public void testTroilkattStatus() throws IOException, TroilkattPropertiesException {
		TroilkattStatus s = new TroilkattStatus(troilkatt.tfs, troilkattProperties);
	
		assertEquals(hdfsStatusPath, s.hdfsStatusPath);
		assertEquals(statusPath, s.statusPath);
		assertEquals(statusFilename, s.statusFilename);		
		assertNotNull(s.logger);
		assertEquals(troilkatt.tfs, s.tfs);
		
		// Test without a file on the local FS
		OsPath.delete(statusFilename);
		s = new TroilkattStatus(troilkatt.tfs, troilkattProperties);
		
		// Test with file that is already on local FS
		s = new TroilkattStatus(troilkatt.tfs, troilkattProperties);
		
		// Test with a filename neither on local FS or remote FS
		troilkattProperties.set("troilkatt.hdfs.status.file", statusFilename + ".2");
		s = new TroilkattStatus(troilkatt.tfs, troilkattProperties);
	}
	
	// Non-existing status file (create new)
	@Test
	public void testTroilkattStatus2() throws IOException, TroilkattPropertiesException {
		troilkattProperties.set("troilkatt.hdfs.status.file", "non-existing");
		String troilkattDir = troilkattProperties.get("troilkatt.localfs.dir");	
		statusFilename = OsPath.join(troilkattDir, "non-existing");		
		TroilkattStatus s = new TroilkattStatus(troilkatt.tfs, troilkattProperties);
	
		//assertEquals(hdfsStatusPath, s.hdfsStatusPath);
		assertEquals(statusPath, s.statusPath);
		assertEquals(statusFilename, s.statusFilename);		
		assertNotNull(s.logger);
		assertEquals(troilkatt.tfs, s.tfs);
		
		// Test without a file on the local FS
		OsPath.delete(statusFilename);
		s = new TroilkattStatus(troilkatt.tfs, troilkattProperties);
		
		// Test with file that is already on local FS
		s = new TroilkattStatus(troilkatt.tfs, troilkattProperties);
		
		// Test with a filename neither on local FS or remote FS
		troilkattProperties.set("troilkatt.hdfs.status.file", statusFilename + ".2");
		s = new TroilkattStatus(troilkatt.tfs, troilkattProperties);
	}
	
	// Invalid filename
	//
	// Note! Not possible to test in pseudo mode since hadoop does not respect directory settings?
	//
	//@Test(expected=TroilkattPropertiesException.class)
	//public void testTroilkattStatus2() throws IOException, TroilkattPropertiesException {
	//	OsPath.delete(OsPath.join(troilkattProperties.get("troilkatt.localfs.dir"), "baz"));
	//	troilkattProperties.set("troilkatt.hdfs.status.file", "/foo/bar/baz");
	//	assertNotNull( new TroilkattStatus(troilkatt.tfs, troilkattProperties) );
	//}

	@Test
	public void testgetTimestamp() throws InterruptedException, IOException, TroilkattPropertiesException {
		long p = -1;
		for (int i = 0; i < 3; i++) {
			long t = TroilkattStatus.getTimestamp();
			assertTrue(t > 0);
			assertTrue(t > p);
			p = t;
			Thread.sleep(1000);
		}
	}

	// timeLong2Str and timeStr2Long are tested together
	@Test
	public void testTimeLong2Str() throws IOException, TroilkattPropertiesException {
		TroilkattStatus s = new TroilkattStatus(troilkatt.tfs, troilkattProperties);
				//
		long timestamp = 1294671060000L;
		String timeStr = "2011-01-10-15:51";
		
		assertEquals(timeStr, s.timeLong2Str(timestamp));
		assertEquals(timestamp, s.timeStr2Long(timeStr));
	}


	@Test
	public void testGetLastStatus() throws IOException, TroilkattPropertiesException {		
		TroilkattStatus s = new TroilkattStatus(troilkatt.tfs, troilkattProperties);
		
		assertEquals("start", s.getLastStatus("Troilkatt"));
		assertEquals("done", s.getLastStatus("theSource"));
		assertEquals("done", s.getLastStatus("firstStage"));
		assertEquals("recover", s.getLastStatus("secondStage"));
		assertEquals("done", s.getLastStatus("thirdStage"));
		assertEquals("done", s.getLastStatus("theSink"));		
		assertNull(s.getLastStatus("nonExistingStage"));		
	}
	
	
	@Test
	public void testGetStatus() throws IOException, TroilkattPropertiesException {
		TroilkattStatus s = new TroilkattStatus(troilkatt.tfs, troilkattProperties);
		
		long timestamp = 3;
		
		assertEquals("done", s.getStatus("Troilkatt", timestamp));
		assertEquals("done", s.getStatus("theSource", timestamp));
		assertEquals("done", s.getStatus("firstStage", timestamp));
		assertEquals("done", s.getStatus("secondStage", timestamp));
		assertEquals("done", s.getStatus("thirdStage", timestamp));
		assertEquals("done", s.getStatus("theSink", timestamp));		
		assertNull(s.getStatus("nonExistingStage", timestamp));
		
		timestamp = 4;
		assertEquals("start", s.getStatus("Troilkatt", timestamp));
		assertEquals("done", s.getStatus("theSource", timestamp));
		assertEquals("done", s.getStatus("firstStage", timestamp));
		assertEquals("recover", s.getStatus("secondStage", timestamp));
		assertNull(s.getStatus("thirdStage", timestamp));
		assertNull(s.getStatus("theSink", timestamp));		
		assertNull(s.getStatus("nonExistingStage", timestamp));
		
		timestamp = 5;
		assertNull(s.getStatus("Troilkatt", timestamp));
	}
	
	// With invalid status file (too many parts in one line)
	@Test
	public void testGeStatus2() throws IOException, TroilkattPropertiesException {	
		ArrayList<String> invalidLine = new ArrayList<String>();
		invalidLine.add("5:invalid:start:tooMuch");
		FSUtils.appendTextFile(statusFilename, invalidLine);		
		String troilkattDir = troilkattProperties.get("troilkatt.localfs.dir");
		OsPath.delete(OsPath.join(troilkattDir, ".status-test.txt.crc"));
		troilkatt.tfs.hdfs.copyFromLocalFile(false, true, statusPath, hdfsStatusPath); // keep local & overwrite
		
		TroilkattStatus s = new TroilkattStatus(troilkatt.tfs, troilkattProperties);		
		long timestamp = 5;		
		assertEquals(null, s.getStatus("invalid", timestamp));
	}

	// With invalid status file (too few parts in one line)
	@Test
	public void testGetStatus3() throws IOException, TroilkattPropertiesException {	
		ArrayList<String> invalidLine = new ArrayList<String>();
		invalidLine.add("5:invalid");
		FSUtils.appendTextFile(statusFilename, invalidLine);		
		String troilkattDir = troilkattProperties.get("troilkatt.localfs.dir");
		OsPath.delete(OsPath.join(troilkattDir, ".status-test.txt.crc"));
		troilkatt.tfs.hdfs.copyFromLocalFile(false, true, statusPath, hdfsStatusPath); // keep local & overwrite
		
		TroilkattStatus s = new TroilkattStatus(troilkatt.tfs, troilkattProperties);		
		long timestamp = 5;		
		assertEquals(null, s.getStatus("invalid", timestamp));
	}

	@Test
	public void testGetLastStatusTimestamp() throws IOException, TroilkattPropertiesException {		
		TroilkattStatus s = new TroilkattStatus(troilkatt.tfs, troilkattProperties);		
		
		assertEquals(4, s.getLastStatusTimestamp("Troilkatt"));
		assertEquals(4, s.getLastStatusTimestamp("theSource"));
		assertEquals(4, s.getLastStatusTimestamp("firstStage"));
		assertEquals(4, s.getLastStatusTimestamp("secondStage"));
		assertEquals(3, s.getLastStatusTimestamp("thirdStage"));
		assertEquals(3, s.getLastStatusTimestamp("theSink"));		
		assertEquals(-1, s.getLastStatusTimestamp("nonExistingStage"));
	}

	@Test
	public void testSetStatus() throws IOException, TroilkattPropertiesException {
		troilkattProperties.set("troilkatt.hdfs.status.file", statusFilename + ".modified");
		TroilkattStatus s = new TroilkattStatus(troilkatt.tfs, troilkattProperties);
		
		s.setStatus("Troilkatt", 10, "start");
		assertEquals("start", s.getStatus("Troilkatt", 10));
		s.setStatus("Troilkatt", 10, "done");
		assertEquals("done", s.getStatus("Troilkatt", 10));
	}

	@Test
	public void testSaveStatusFile() throws IOException, TroilkattPropertiesException {
		String modifiedName = troilkattProperties.get("troilkatt.hdfs.status.file") + ".modified";
		
		troilkattProperties.set("troilkatt.hdfs.status.file", modifiedName);
		TroilkattStatus s = new TroilkattStatus(troilkatt.tfs, troilkattProperties);
		
		s.setStatus("secondStage", 4, "done");		
		s.setStatus("thirdStage", 4, "recover");
		OsPath.copy(statusFilename + ".modified", statusFilename + ".modified2");
		s.saveStatusFile();
		
		OsPath.delete(statusFilename + ".modified");
		// will download deleted file
		s = new TroilkattStatus(troilkatt.tfs, troilkattProperties);
		// Compare files, line by line
		BufferedReader is1 = new BufferedReader(new FileReader(statusFilename + ".modified"));
		BufferedReader is2 = new BufferedReader(new FileReader(statusFilename + ".modified2"));

		while (true) {
			String l1 = is1.readLine();
			String l2 = is2.readLine();

			if ((l1 == null) && (l2 == null)) {
				break;
			}
			else if ((l1 == null) || (l2 == null)) {
				fail("Files differ");
			}
			if (! l1.equals(l2)) {
				fail("Files differ");
			}								
		}   
		is1.close();
		is2.close();
	}

}
