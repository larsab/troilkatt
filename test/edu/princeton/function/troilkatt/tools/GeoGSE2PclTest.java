package edu.princeton.function.troilkatt.tools;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.princeton.function.troilkatt.TestSuper;
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.OsPath;

public class GeoGSE2PclTest extends TestSuper {

	protected static String inputFilename;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		inputFilename = OsPath.join(dataDir, "files/GSE8070_family.soft");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		OsPath.mkdir(tmpDir);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testGeoGSE2Pcl() {
		@SuppressWarnings("unused")
		GeoGSE2Pcl parser = new GeoGSE2Pcl();
		// No test of initialized values
	}

	// Tested as part of getMetaLines
	//@Test
	//public void testGeoGSE2PclArrayListOfString() {
	//}

	// Not tested
	//@Test
	//public void testReset() {
	//}

	@Test
	public void testParseLine1() throws IOException, ParseException {
		GeoGSE2Pcl parser = new GeoGSE2Pcl();
		
		BufferedReader ins = new BufferedReader(new FileReader(inputFilename));		
		parser.stage1(ins, null);		
		verifyMeta(parser);
		
		// Rerun, but outptu results to a file
		FileOutputStream fos = new FileOutputStream(OsPath.join(tmpDir, "stage1.ser"));
		parser = new GeoGSE2Pcl();		
		ins = new BufferedReader(new FileReader(inputFilename));		
		parser.stage1(ins, fos);		
		verifyMeta(parser);
	}
	
	private void verifyMeta(GeoGSE2Pcl parser) {
		assertEquals(3, parser.platformIDColumns.size());		
		assertTrue(parser.platformIDColumns.containsKey("GPL81"));
		assertTrue(parser.platformIDColumns.get("GPL81") == 0);
		assertTrue(parser.platformIDColumns.containsKey("GPL82"));
		assertTrue(parser.platformIDColumns.get("GPL82") == 0);
		assertTrue(parser.platformIDColumns.containsKey("GPL83"));
		assertTrue(parser.platformIDColumns.get("GPL83") == 0);
		
		assertEquals(3, parser.platformGeneNameColumns.size());
		assertTrue(parser.platformGeneNameColumns.containsKey("GPL81"));		
		assertTrue(10 == parser.platformGeneNameColumns.get("GPL81"));
		assertTrue(parser.platformGeneNameColumns.containsKey("GPL82"));
		assertTrue(10 == parser.platformGeneNameColumns.get("GPL82"));
		assertTrue(parser.platformGeneNameColumns.containsKey("GPL83"));
		assertTrue(10 == parser.platformGeneNameColumns.get("GPL83"));
		
		assertEquals(30, parser.sampleGeneIDColumns.size());
		assertTrue(parser.sampleGeneIDColumns.containsKey("GSM199443"));
		assertTrue(0 == parser.sampleGeneIDColumns.get("GSM199443"));
		assertTrue(parser.sampleGeneIDColumns.containsKey("GSM199447"));
		assertTrue(0 == parser.sampleGeneIDColumns.get("GSM199447"));
		assertTrue(parser.sampleGeneIDColumns.containsKey("GSM199472"));
		assertTrue(0 == parser.sampleGeneIDColumns.get("GSM199472"));
		
		assertEquals(30, parser.sampleValueColumns.size());
		assertTrue(parser.sampleValueColumns.containsKey("GSM199443"));
		assertTrue(1 == parser.sampleValueColumns.get("GSM199443"));
		assertTrue(parser.sampleValueColumns.containsKey("GSM199447"));
		assertTrue(1 == parser.sampleValueColumns.get("GSM199447"));
		assertTrue(parser.sampleValueColumns.containsKey("GSM199472"));
		assertTrue(1 == parser.sampleValueColumns.get("GSM199472"));
		
		assertEquals(3, parser.platformSampleIDs.size());
		assertTrue(parser.platformSampleIDs.containsKey("GPL81"));
		assertEquals(10, parser.platformSampleIDs.get("GPL81").size());
		assertTrue(parser.platformSampleIDs.containsKey("GPL81"));
		assertEquals(10, parser.platformSampleIDs.get("GPL81").size());
		assertTrue(parser.platformSampleIDs.containsKey("GPL81"));
		assertEquals(10, parser.platformSampleIDs.get("GPL81").size());
		
		assertEquals(30, parser.sampleTitles.size());
		assertTrue(parser.sampleTitles.containsKey("GSM199443"));
		assertEquals("Pancreas at e12.5, Chip A, rep 1", parser.sampleTitles.get("GSM199443"));
		assertTrue(parser.sampleTitles.containsKey("GSM199447"));
		assertEquals("Pancreas at e14.5, Chip A, rep 1", parser.sampleTitles.get("GSM199447"));
		assertTrue(parser.sampleTitles.containsKey("GSM199472"));
		System.out.println( parser.sampleTitles.get("GSM199472"));
		assertEquals("Pancreas at e16.5, Chip C, rep 2", parser.sampleTitles.get("GSM199472"));
	
		assertEquals(30, parser.orderedSampleIDs.size());
		assertEquals(0, parser.orderedSampleIDs.indexOf("GSM199443"));
		assertEquals(4, parser.orderedSampleIDs.indexOf("GSM199447"));
		assertEquals(29, parser.orderedSampleIDs.indexOf("GSM199472"));
		
		assertEquals(3, parser.orderedPlatformIDs.size());
		assertEquals(0, parser.orderedPlatformIDs.indexOf("GPL81"));
		assertEquals(1, parser.orderedPlatformIDs.indexOf("GPL82"));
		assertEquals(2, parser.orderedPlatformIDs.indexOf("GPL83"));
	}

	@Test
	public void testParseLine2() throws ParseException, IOException, ClassNotFoundException {
		GeoGSE2Pcl parser = new GeoGSE2Pcl();
		
		BufferedReader ins = new BufferedReader(new FileReader(inputFilename));
		parser.stage1(ins, null);		
		ins.close();

		ins = new BufferedReader(new FileReader(inputFilename));
		parser.stage2(ins, null, "GPL82");		
		ins.close();
		
		assertEquals(12477, parser.geneID2Name.size());
		HashMap<String, float[]> id2val = parser.geneID2ValsSoftReference.get();
		assertNotNull(id2val);
		assertEquals(12477, id2val.size());
		
		/*
		 *  Re-run experiment, this time using the serialization file between stage  1 and 2
		 */
		parser = new GeoGSE2Pcl();
		ins = new BufferedReader(new FileReader(inputFilename));
		String serFilename = OsPath.join(tmpDir, "stage1.ser");
		FileOutputStream fos = new FileOutputStream(serFilename);
		parser.stage1(ins, fos);		
		ins.close();
		fos.close();
		
		// Create new parser and initialize using the serial file written in stage1()
		parser = new GeoGSE2Pcl();
		FileInputStream fis = new FileInputStream(serFilename);
		parser.readStage1Results(fis);
		fis.close();
		
		ins = new BufferedReader(new FileReader(inputFilename));
		parser.stage2(ins, null, "GPL82");		
		ins.close();
		
		assertEquals(12477, parser.geneID2Name.size());
		id2val = parser.geneID2ValsSoftReference.get();
		assertNotNull(id2val);
		assertEquals(12477, id2val.size());
	}

	@Test
	public void testGetOutputLines() throws IOException, ParseException {
		GeoGSE2Pcl parser = new GeoGSE2Pcl();
		
		BufferedReader ins = new BufferedReader(new FileReader(inputFilename));
		parser.stage1(ins, null);		
		ins.close();

		ins = new BufferedReader(new FileReader(inputFilename));
		parser.stage2(ins, null, "GPL82");		
		ins.close();
		
		OsPath.mkdir(tmpDir);
		String outputFilename = OsPath.join(tmpDir, "GSE8070.pcl");
		BufferedWriter os = new BufferedWriter(new FileWriter(outputFilename));
		parser.writeOutputLines(os, "GPL82");
		os.close();
		
		String[] outputLines = FSUtils.readTextFile(outputFilename);
		
		assertEquals(12477 + 2, outputLines.length);
		String firstLine = outputLines[0];
		assertTrue(firstLine.startsWith("ID_REF\tGENE_NAME\tGWEIGHT"));
		String[] cols = firstLine.split("\t"); 
		assertEquals(10 + 3, cols.length);
		assertEquals("GSM199453: Pancreas at e12.5, Chip B, rep 1", cols[3]);		
		assertEquals("GSM199457: Pancreas at e14.5, Chip B, rep 1", cols[7]);		
		
		String secondLine = outputLines[1];
		assertTrue(secondLine.startsWith("EWEIGHT\t\t"));
		cols = secondLine.split("\t");
		assertEquals(13, cols.length);
		assertEquals("1", cols[3]);		
		assertEquals("1", cols[7]);		
		assertEquals("1", cols[12]);
		
		String dataLine = outputLines[2];		
		cols = dataLine.split("\t");
		assertEquals(13, cols.length);
		assertEquals("104769_at", cols[0]);
		assertEquals("LIMA1", cols[1]);
		assertEquals("1", cols[2]);
		assertEquals("3.2483451", cols[3]);		
		assertEquals("3.3340945", cols[4]);		
		assertEquals("3.5329928", cols[12]);
		
		dataLine = outputLines[4785];
		cols = dataLine.split("\t");
		assertEquals(13, cols.length);
		assertEquals("111524_at", cols[0]);
		assertEquals("ARID4B", cols[1]);
		assertEquals("1", cols[2]);
		assertEquals("7.001966", cols[3]);		
		assertEquals("6.999243", cols[4]);		
		assertEquals("6.4580564", cols[12]);
		
		/*
		 * Re-run, this time without an explicit call to writeOutputLines
		 */
		parser = new GeoGSE2Pcl();
		
		ins = new BufferedReader(new FileReader(inputFilename));
		parser.stage1(ins, null);		
		ins.close();

		ins = new BufferedReader(new FileReader(inputFilename));
		OsPath.delete(outputFilename);
		os = new BufferedWriter(new FileWriter(outputFilename));
		parser.stage2(ins, os, "GPL82");		
		ins.close();
		os.close();

		outputLines = FSUtils.readTextFile(outputFilename);
		
		assertEquals(12477 + 2, outputLines.length);
		firstLine = outputLines[0];
		assertTrue(firstLine.startsWith("ID_REF\tGENE_NAME\tGWEIGHT"));
		cols = firstLine.split("\t"); 
		assertEquals(10 + 3, cols.length);
		assertEquals("GSM199453: Pancreas at e12.5, Chip B, rep 1", cols[3]);		
		assertEquals("GSM199457: Pancreas at e14.5, Chip B, rep 1", cols[7]);		
		
		secondLine = outputLines[1];
		assertTrue(secondLine.startsWith("EWEIGHT\t\t"));
		cols = secondLine.split("\t");
		assertEquals(13, cols.length);
		assertEquals("1", cols[3]);		
		assertEquals("1", cols[7]);		
		assertEquals("1", cols[12]);
		
		dataLine = outputLines[2];		
		cols = dataLine.split("\t");
		assertEquals(13, cols.length);
		assertEquals("104769_at", cols[0]);
		assertEquals("LIMA1", cols[1]);
		assertEquals("1", cols[2]);
		assertEquals("3.2483451", cols[3]);		
		assertEquals("3.3340945", cols[4]);		
		assertEquals("3.5329928", cols[12]);
		
		dataLine = outputLines[4785];
		cols = dataLine.split("\t");
		assertEquals(13, cols.length);
		assertEquals("111524_at", cols[0]);
		assertEquals("ARID4B", cols[1]);
		assertEquals("1", cols[2]);
		assertEquals("7.001966", cols[3]);		
		assertEquals("6.999243", cols[4]);		
		assertEquals("6.4580564", cols[12]);
		
		/*	
		 * 		outputLines = parser.getOutputLines("GPL81");
		writeOutputFile(outputLines);
		
			parser.getOutputLines("GPL81", false, false);
		writeOutputFile(outputLines);
		
		dataLine = outputLines.get(2);		
		cols = dataLine.split("\t");
		assertEquals(13, cols.length);
		assertEquals("100001_at", cols[0]);
		assertEquals("CD3G", cols[1]);
		assertEquals("1", cols[2]);
		assertEquals("3.520287082", cols[3]);		
		assertEquals("3.557308359", cols[4]);		
		assertEquals("3.527395518\n", cols[12]);
		
		dataLine = outputLines.get(9272);
		cols = dataLine.split("\t");
		assertEquals(13, cols.length);
		assertEquals("95695_at", cols[0]);
		assertEquals("SLC25A20", cols[1]);
		assertEquals("1", cols[2]);
		assertEquals("6.737577719", cols[3]);		
		assertEquals("6.787799865", cols[4]);		
		assertEquals("6.561904137\n", cols[12]);
		
		outputLines = 
		
		outputLines = parser2.getOutputLines(null, true, true);
		writeOutputFile(outputLines);
		
		firstLine = outputLines.get(0).trim();
		assertTrue(firstLine.startsWith("ID_REF\tGENE_NAME\tGWEIGHT"));
		cols = firstLine.split("\t"); 
		assertEquals(33, cols.length);
		assertEquals("Pancreas at e12.5, Chip A, rep 1", cols[3]);		
		assertEquals("Pancreas at e14.5, Chip A, rep 1", cols[7]);		
		assertEquals("Pancreas at e16.5, Chip C, rep 2", cols[32]);
		
		secondLine = outputLines.get(1).trim();
		assertTrue(secondLine.startsWith("EWEIGHT\t\t"));
		cols = secondLine.split("\t");
		assertEquals(33, cols.length);
		assertEquals("1", cols[3]);		
		assertEquals("1", cols[7]);		
		assertEquals("1", cols[32]);
		
		dataLine = outputLines.get(4719);		
		cols = dataLine.split("\t");
		assertEquals(33, cols.length);
		assertEquals("CD3G", cols[0]);
		assertEquals("CD3G", cols[1]);
		assertEquals("1", cols[2]);
		assertEquals("3.520287082", cols[3]);		
		assertEquals("3.557308359", cols[4]);		
		assertEquals("3.527395518", cols[12]);
		
		dataLine = outputLines.get(4717);
		cols = dataLine.split("\t");
		assertEquals(33, cols.length);
		assertEquals("CD3D", cols[0]);
		assertEquals("CD3D", cols[1]);
		assertEquals("1", cols[2]);
		assertEquals("3.055146756", cols[3]);		
		assertEquals("2.929735171", cols[4]);		
		assertEquals("3.482231645\n", cols[32]);*/
	}

	@Test
	public void testGetKey() {
		assertEquals("key", GeoGSE2Pcl.getKey("key = val"));
		assertEquals("key", GeoGSE2Pcl.getKey("key=val"));
		assertEquals("key", GeoGSE2Pcl.getKey(" key=val "));
		assertNull(GeoGSE2Pcl.getKey("key="));
		assertEquals("key", GeoGSE2Pcl.getKey("key=val\n"));
		assertNull(GeoGSE2Pcl.getKey("key"));
		assertNull(GeoGSE2Pcl.getKey("val"));
		assertNull(GeoGSE2Pcl.getKey("key=val1=val2"));
	}

	@Test
	public void testGetVal() {
		assertEquals("val", GeoGSE2Pcl.getVal("key = val"));
		assertEquals("val", GeoGSE2Pcl.getVal("key=val"));
		assertEquals("val", GeoGSE2Pcl.getVal(" key=val "));
		assertEquals("val", GeoGSE2Pcl.getVal("key=val\n"));
		assertNull(GeoGSE2Pcl.getVal("key="));
		assertNull(GeoGSE2Pcl.getVal("key"));
		assertNull(GeoGSE2Pcl.getVal("val"));
		assertNull(GeoGSE2Pcl.getVal("key=val1=val2"));
	}

	@Test
	public void testIsNumeric() {
		assertTrue(GeoGSE2Pcl.isNumeric("12.3"));
		assertTrue(GeoGSE2Pcl.isNumeric("0"));
		assertTrue(GeoGSE2Pcl.isNumeric("-0.3"));
		assertTrue(GeoGSE2Pcl.isNumeric("12e2"));
		assertFalse(GeoGSE2Pcl.isNumeric(""));
		assertTrue(GeoGSE2Pcl.isNumeric("NaN"));
		assertFalse(GeoGSE2Pcl.isNumeric("None"));
		assertFalse(GeoGSE2Pcl.isNumeric("null"));
	}

	// getPlatformID + getDsetID
	@Test
	public void testGeters() throws ParseException, IOException {
		GeoGSE2Pcl parser = new GeoGSE2Pcl();
		
		BufferedReader ins = new BufferedReader(new FileReader(inputFilename));
		parser.stage1(ins, null);		
		ins.close();

		ins = new BufferedReader(new FileReader(inputFilename));
		parser.stage2(ins, null, "GPL82");		
		ins.close();
		
		ArrayList<String> platformIDs = parser.getPlatformIDs();
		assertEquals(3, platformIDs.size());
		assertEquals("GPL81", platformIDs.get(0));
		assertEquals("GPL82", platformIDs.get(1));
		assertEquals("GPL83", platformIDs.get(2));
		
		assertEquals("GSE8070", parser.getDsetID());
	}
	// Tested as part of getMetaLines
	//@Test
	//public void testGeoGSE2PclArrayListOfString() {
	//}
}
