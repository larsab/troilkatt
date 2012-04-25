package edu.princeton.function.troilkatt.tools;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.princeton.function.troilkatt.TestSuper;
import edu.princeton.function.troilkatt.fs.OsPath;

public class Pcl2InfoTest extends TestSuper {

	protected static String inputFilename;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		inputFilename = OsPath.join(dataDir, "files/GDS2924.pcl");
	}


	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testPcl2Info() {
		@SuppressWarnings("unused")
		Pcl2Info parser = new Pcl2Info();
	}

	// Not tested separately
	//@Test
	//public void testCalculateLine() {
	//	Pcl2Info parser = new Pcl2Info();
	//	
	//}

	// Not tested separately
	//@Test
	//public void testGetResults() {
	//}
	
	@Test
	public void testCalculate() throws IOException, ParseException {
		Pcl2Info parser = new Pcl2Info();
		BufferedReader ins = new BufferedReader(new FileReader(inputFilename));
		
		HashMap<String, String> results = parser.calculate(ins);
		assertEquals(12, results.size());
		
		assertEquals("2.181", results.get("min"));
		assertEquals("12.88", results.get("max"));
		assertTrue(results.get("mean").startsWith("5.31"));
		assertEquals("0", results.get("numNeg"));
		assertEquals("124110", results.get("numPos"));
		assertEquals("0", results.get("numZero"));
		assertEquals("660", results.get("numMissing"));
		assertEquals("124110", results.get("numTotal"));
		
		assertEquals("1", results.get("channels"));
		assertEquals("1", results.get("logged"));
		assertEquals("0", results.get("zerosAreMVs"));
		assertEquals("0", results.get("cutoff"));
		
		parser.reset();
		assertFalse(parser.headerLineRead);
		assertFalse(parser.eWeightLineRead);
		assertTrue(1e10 == parser.min);
	}

	// Not tested separately
	//@Test
	//public void testReset() {
	//	Pcl2Info parser = new Pcl2Info();
	//	
	//}
}
