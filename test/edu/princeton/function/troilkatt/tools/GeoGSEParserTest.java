package edu.princeton.function.troilkatt.tools;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.princeton.function.troilkatt.TestSuper;
import edu.princeton.function.troilkatt.fs.OsPath;

public class GeoGSEParserTest extends TestSuper {

	protected static String gseFile;
	
	protected String line1;
	protected String line2;
	protected String line3;
	protected String line4;
	protected String line5;
	protected String line6;
	protected String line7;
	protected String line8;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		gseFile = OsPath.join(dataDir, "files/GSE8070_family.soft");
		line1 = "!Series_title = Expression profiling of pancreas development";
		line2 = "!Platform_geo_accession = GPL81";
		line3 = "100106_at	D38410		Mus musculus	Mar 11, 2009	Consensus sequence	GenBank	Cluster Incl D38410:Trefoil factor 3, intestinal /cds=(35,280) /gb=D38410 /gi=551626 /ug=Mm.4641 /len=450	D38410	trefoil factor 3, intestinal	Tff3	21786	NM_011575		0005576 // extracellular region // inferred from electronic annotation /// 0030141 // secretory granule // inferred from direct assay";
		line4 = "!Database_email = geo@ncbi.nlm.nih.gov";
		line5 = "foo";
		line6 = "!Sample_geo_accession = GSM199443";
		line7 = "!Sample_geo_accession = GSM199444";
		line8 = "!Sample_geo_accession = GSM199445";
	}

	@After
	public void tearDown() throws Exception {
	}

	// Also implicitly checks setupTags() and checkTags()
	@Test
	public void testGeoGSEParser() {
		// The constructor calls setupTags() and then checkTags()
		// which contains error checks for the data structures
		// initialized in setupTags()
		GeoGSEParser parser = new GeoGSEParser();
		assertNotNull(parser);
	}

	@Test
	public void testParseLine() {
		GeoGSEParser parser = new GeoGSEParser();
		assertTrue(parser.parseLine(line1));
		assertTrue(parser.parseLine(line2));
		assertFalse(parser.parseLine(line3));
		assertFalse(parser.parseLine(line4));
		assertFalse(parser.parseLine(line5));
		assertTrue(parser.parseLine(line6));
		assertTrue(parser.parseLine(line7));
		assertTrue(parser.parseLine(line8));
	}

	@Test
	public void testGetMeta() {
		GeoGSEParser parser = new GeoGSEParser();
		assertTrue(parser.parseLine(line1));
		assertTrue(parser.parseLine(line2));
		assertTrue(parser.parseLine(line6));
		assertTrue(parser.parseLine(line7));
		assertTrue(parser.parseLine(line8));

		HashMap<String, ArrayList<String>> meta = parser.getMeta();
		assertEquals(3, meta.size());
	}

	@Test
	public void testGetSingleValue() throws ParseException {
		GeoGSEParser parser = new GeoGSEParser();
		assertTrue(parser.parseLine(line1));
		assertTrue(parser.parseLine(line2));
		assertTrue(parser.parseLine(line6));
		assertTrue(parser.parseLine(line7));
		assertTrue(parser.parseLine(line8));

		String val = parser.getSingleValue("title");
		assertEquals("Expression profiling of pancreas development", val);
	}
	
	@Test(expected = ParseException.class)
	public void testGetSingleValue3() throws ParseException {
		GeoGSEParser parser = new GeoGSEParser();	
		assertNull(parser.getSingleValue("pmid"));
	}

	@Test(expected = ParseException.class)
	public void testGetSingleValue2() throws ParseException {
		GeoGSEParser parser = new GeoGSEParser();
		assertTrue(parser.parseLine(line6));

		assertNotNull(parser.getSingleValue("sampleIDs"));
	}

	@Test
	public void testGetValues() {
		GeoGSEParser parser = new GeoGSEParser();
		assertTrue(parser.parseLine(line1));
		assertTrue(parser.parseLine(line2));
		assertTrue(parser.parseLine(line6));
		assertTrue(parser.parseLine(line7));
		assertTrue(parser.parseLine(line8));

		ArrayList<String> vals = parser.getValues("title");
		assertEquals(1, vals.size());
		assertEquals("Expression profiling of pancreas development", vals.get(0));

		assertNull(parser.getValues("pmid"));

		vals = parser.getValues("sampleIDs");
		assertEquals(3, vals.size());
		Collections.sort(vals);
		assertEquals("GSM199443", vals.get(0));
		assertEquals("GSM199445", vals.get(2));
	}

	@Test
	public void testAFile() throws IOException, ParseException {
		GeoGSEParser parser = new GeoGSEParser();
		BufferedReader ib = new BufferedReader(new FileReader(gseFile));

		String line;
		while ((line = ib.readLine()) != null) {
			if ((line.charAt(0) == '!') || (line.charAt(0) == '^')) {
				parser.parseLine(line);
			}			
		}
		ib.close();
		
		assertEquals("GSE8070", parser.getSingleValue("id"));
		assertEquals("Expression profiling of pancreas development", parser.getSingleValue("title"));
		assertEquals("Dec 10 2010", parser.getSingleValue("date"));
		//assertNull(parser.getSingleValue("pmid"));		
		String val = parser.getSingleValue("description");		
		assertTrue(val.startsWith("Development of the pancreas"));
				
		ArrayList<String> vals = parser.getValues("organisms");
		assertEquals(1, vals.size());
		assertEquals("Mus musculus", vals.get(0));
		int nPlatforms = 3;
		vals = parser.getValues("platformIDs");
		assertEquals(nPlatforms, vals.size());
		assertEquals("GPL82", vals.get(1));
		vals = parser.getValues("platformTitles");
		assertEquals(nPlatforms, vals.size());
		assertEquals("[MG_U74Av2] Affymetrix Murine Genome U74 Version 2 Array", vals.get(0));
		vals = parser.getValues("rowCounts");
		assertEquals(nPlatforms, vals.size());
		assertEquals("12477", vals.get(1));
		int nSamples = 30;
		vals = parser.getValues("sampleIDs");
		Collections.sort(vals);
		assertEquals(nSamples, vals.size());
		assertEquals("GSM199446", vals.get(3));
		vals = parser.getValues("sampleTitles");
		assertEquals(nSamples, vals.size());
		assertEquals("Pancreas at e12.5, Chip A, rep 1", vals.get(0));
		vals = parser.getValues("channelCounts");
		assertEquals(1, vals.size());
		assertEquals("1", vals.get(0));
		assertNull(parser.getValues("valueTypes"));		
	}
}
