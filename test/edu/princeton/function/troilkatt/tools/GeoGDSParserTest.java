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

public class GeoGDSParserTest extends TestSuper {
	protected static String gdsFile;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		gdsFile = OsPath.join(dataDir, "files/GDS2949_full.soft");
	}

	@After
	public void tearDown() throws Exception {
	}

	// Also implicitly checks setupTags() and checkTags()
	@Test
	public void testGeoGDSParser() {
		// The constructor calls setupTags() and then checkTags()
		// which contains error checks for the data structures
		// initialized in setupTags()
		GeoGDSParser parser = new GeoGDSParser();
		assertNotNull(parser);
	}

	@Test
	public void testParseLine() {
		String line1 = "!dataset_title = Pancreatic development (MG-U74B)";
		String line2 = "!dataset_platform = GPL82";
		String line3 = "cDNA clone IMAGE:1023838 3- similar to gb:M64086 Mouse spi2 proteinase inhibitor (MOUSE);, mRNA sequence	4404405	AI506554				12	Chromosome 12, NC_000078.5 (105552780..105558138)	molecular_function	biological_process	cellular_component	GO:0003674	GO:0008150	GO:0005575";
		String line4 = "!Database_email = geo@ncbi.nlm.nih.gov";
		String line5 = "foo";
		String line6 = "!subset_sample_id = GSM199453,GSM199454";
		String line7 = "!subset_sample_id = GSM199455,GSM199456";
		String line8 = "!subset_sample_id = GSM199457,GSM199458";

		GeoGDSParser parser = new GeoGDSParser();
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
		String line1 = "!dataset_title = Pancreatic development (MG-U74B)";
		String line2 = "!dataset_platform = GPL82";
		String line6 = "!subset_sample_id = GSM199453,GSM199454";
		String line7 = "!subset_sample_id = GSM199455,GSM199456";
		String line8 = "!subset_sample_id = GSM199457,GSM199458";

		GeoGDSParser parser = new GeoGDSParser();
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
		String line1 = "!dataset_title = Pancreatic development (MG-U74B)";
		String line2 = "!dataset_platform = GPL82";
		String line6 = "!subset_sample_id = GSM199453,GSM199454";
		String line7 = "!subset_sample_id = GSM199455,GSM199456";
		String line8 = "!subset_sample_id = GSM199457,GSM199458";

		GeoGDSParser parser = new GeoGDSParser();
		assertTrue(parser.parseLine(line1));
		assertTrue(parser.parseLine(line2));
		assertTrue(parser.parseLine(line6));
		assertTrue(parser.parseLine(line7));
		assertTrue(parser.parseLine(line8));

		String val = parser.getSingleValue("title");
		assertEquals("Pancreatic development (MG-U74B)", val);
	}
	
	@Test(expected = ParseException.class)
	public void testGetSingleValue3() throws ParseException {
		GeoGDSParser parser = new GeoGDSParser();		
		parser.getSingleValue("pmid");
	}

	@Test(expected = ParseException.class)
	public void testGetSingleValue2() throws ParseException {
		String line6 = "!subset_sample_id = GSM199453,GSM199454";

		GeoGDSParser parser = new GeoGDSParser();
		assertTrue(parser.parseLine(line6));

		assertNotNull(parser.getSingleValue("sampleIDs"));
	}

	@Test
	public void testGetValues() {
		String line1 = "!dataset_title = Pancreatic development (MG-U74B)";
		String line2 = "!dataset_platform = GPL82";
		String line6 = "!subset_sample_id = GSM199453,GSM199454";
		String line7 = "!subset_sample_id = GSM199455,GSM199456";
		String line8 = "!subset_sample_id = GSM199457,GSM199458";

		GeoGDSParser parser = new GeoGDSParser();
		assertTrue(parser.parseLine(line1));
		assertTrue(parser.parseLine(line2));
		assertTrue(parser.parseLine(line6));
		assertTrue(parser.parseLine(line7));
		assertTrue(parser.parseLine(line8));

		ArrayList<String> vals = parser.getValues("title");
		assertEquals(1, vals.size());
		assertEquals("Pancreatic development (MG-U74B)", vals.get(0));

		assertNull(parser.getValues("pmid"));

		vals = parser.getValues("sampleIDs");
		assertEquals(6, vals.size());
		Collections.sort(vals);
		assertEquals("GSM199453", vals.get(0));
		assertEquals("GSM199456", vals.get(3));
	}

	@Test
	public void testAFile() throws IOException, ParseException {
		GeoGDSParser parser = new GeoGDSParser();
		BufferedReader ib = new BufferedReader(new FileReader(gdsFile));

		String line;
		while ((line = ib.readLine()) != null) {
			if ((line.charAt(0) == '!') || (line.charAt(0) == '^')) {
				parser.parseLine(line);
			}			
		}
		ib.close();		
		
		assertEquals("GDS2949", parser.getSingleValue("id"));
		assertEquals("Pancreatic development (MG-U74B)", parser.getSingleValue("title"));
		assertEquals("Sep 11 2007", parser.getSingleValue("date"));
//		assertNull(parser.getSingleValue("pmid"));
		assertEquals("Analysis of pancreatic tissues from NMRI animals from embryonic day E12.5 to E16.5. Results provide insight into the molecular mechanisms underlying the development of the pancreas.", parser.getSingleValue("description"));
		
		ArrayList<String> vals = parser.getValues("organisms");
		assertEquals(1, vals.size());
		assertEquals("Mus musculus", vals.get(0));
		vals = parser.getValues("platformIDs");
		assertEquals(1, vals.size());
		assertEquals("GPL82", vals.get(0));
		vals = parser.getValues("platformTitles");
		assertEquals(1, vals.size());
		assertEquals("in situ oligonucleotide", vals.get(0));
		vals = parser.getValues("rowCounts");
		assertEquals(1, vals.size());
		assertEquals("12477", vals.get(0));
		vals = parser.getValues("sampleIDs");
		int nSamples = vals.size();
		Collections.sort(vals);
		assertEquals(nSamples, vals.size());
		assertEquals("GSM199456", vals.get(3));
		vals = parser.getValues("sampleTitles");
		assertEquals(5, vals.size());
		assertEquals("E13.5", vals.get(1));
		vals = parser.getValues("channelCounts");
		assertEquals(1, vals.size());
		assertEquals("1", vals.get(0));
		vals = parser.getValues("valueTypes");
		assertEquals(1, vals.size());
		assertEquals("transformed count", vals.get(0));
	}
}
