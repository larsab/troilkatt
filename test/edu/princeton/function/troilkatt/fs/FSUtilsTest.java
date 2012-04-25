package edu.princeton.function.troilkatt.fs;

import static org.junit.Assert.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.princeton.function.troilkatt.TestSuper;

public class FSUtilsTest extends TestSuper {
	
	
	protected static String textFile;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {		
		OsPath.mkdir(tmpDir);
		OsPath.mkdir(outDir);
		
		textFile = OsPath.join(outDir, "testfile.txt");
		File file = new File(textFile);
		BufferedWriter os = new BufferedWriter(new FileWriter(file));
		for (String s: linesOfText) {
			os.write(s + "\n");
		}
		os.close();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		OsPath.deleteAll(outDir);
	}

	@Before
	public void setUp() throws Exception {
		OsPath.deleteAll(tmpDir);		
		if (! OsPath.isdir(tmpDir)) {
			OsPath.mkdir(tmpDir);
		}
	}

	@After
	public void tearDown() throws Exception {
		OsPath.deleteAll(tmpDir);		
	}

	// Test for readFile and writeFile
	@Test
	public void testReadWriteBytesFile() throws IOException {
		byte[] inData = FSUtils.readFile(textFile);
		String outFile = OsPath.join(tmpDir, "copy");
		FSUtils.writeFile(outFile, inData);		
		assertTrue(fileCmp(textFile, outFile));
	}
	
	@Test
	public void testReadTextFile() throws IOException {
		String [] lines = FSUtils.readTextFile(textFile);
		
		assertEquals(lines.length, linesOfText.length);
		for (int i = 0; i < lines.length; i++) {
			assertEquals(lines[i], linesOfText[i]);
		}
	}

	@Test
	public void testWriteTextFileStringStringArray() throws IOException {
		String outFile = OsPath.join(tmpDir, "written");
		FSUtils.writeTextFile(outFile, linesOfText);
		assertTrue(fileCmp(textFile, outFile));
	}

	@Test
	public void testWriteTextFileStringArrayListOfString() throws IOException {
		String outFile = OsPath.join(tmpDir, "written");
		
		ArrayList<String> arrayOfText = new ArrayList<String>();
		for (String l: linesOfText) {
			arrayOfText.add(l);
		}
		
		FSUtils.writeTextFile(outFile, arrayOfText);
		assertTrue(fileCmp(textFile, outFile));
	}

	@Test
	public void testAppendTextFile() throws IOException {
		String outFile = OsPath.join(tmpDir, "written");
		ArrayList<String> oneLine= new ArrayList<String>();
		
		// Write first line, then append remaining lines
		oneLine.add(linesOfText[0]);
		FSUtils.writeTextFile(outFile, oneLine);		
		for (int i = 1; i < linesOfText.length; i++) {
			oneLine.clear();
			oneLine.add(linesOfText[i]);
			FSUtils.appendTextFile(outFile, oneLine);
		}		
		assertTrue(fileCmp(textFile, outFile));
	}

	@Test
	public void testSha1file() throws IOException {
		assertEquals("91e053a7813b2fb9bd70942218026bbd8d13dc85", FSUtils.sha1file(OsPath.join(dataDir, "files/file1")));
	}
}
