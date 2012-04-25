package edu.princeton.function.troilkatt.tools;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.princeton.function.troilkatt.TestSuper;
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.OsPath;

public class GeoGDS2PclTest extends TestSuper {

	protected static String inputFilename;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		inputFilename = OsPath.join(dataDir, "files/GDS2949_full.soft");
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
	public void testGeoGDS2Pcl() {
		GeoGDS2Pcl soft2pcl= new GeoGDS2Pcl();
		assertFalse(soft2pcl.atLabels);
		assertFalse(soft2pcl.inDataSection);
		assertTrue(soft2pcl.labels.isEmpty());
	}

	
	@Test
	public void testConvertLine() throws ParseException {
		GeoGDS2Pcl soft2pcl= new GeoGDS2Pcl();
		
		// Parse meta tags
		assertNull(soft2pcl.convertLine("^DATABASE = Geo"));
		assertNull(soft2pcl.convertLine("!dataset_reference_series = GSE8070"));
		// Invalid samples
		assertNull(soft2pcl.convertLine("!dataset_sample_count"));
		assertEquals(0, soft2pcl.nSamples);
		// Valid samples
		assertNull(soft2pcl.convertLine("!dataset_sample_count = 2"));
		assertEquals(2, soft2pcl.nSamples);
	
		// Parse labels
		assertNull(soft2pcl.convertLine("#ID_REF = Platform reference identifier"));
		assertNull(soft2pcl.convertLine("#IDENTIFIER = identifier"));
		assertNull(soft2pcl.convertLine("#GSM199453 = Value for GSM199453: Pancreas at e12.5, Chip B, rep 1; src: Pancreas at e12.5"));
		assertNull(soft2pcl.convertLine("#GSM199454 = Value for GSM199454: Pancreas at e12.5, Chip B, rep 2; src: Pancreas at e12.5"));
		// Invalid label
		int nLabels = soft2pcl.labels.size();
		assertNull(soft2pcl.convertLine("#GSM199454"));
		assertEquals(nLabels, soft2pcl.labels.size());
		
		assertNull(soft2pcl.convertLine("!dataset_table_begin"));
		assertTrue(soft2pcl.atLabels);
		assertFalse(soft2pcl.inDataSection);
		
		// Parse header line
		String line = soft2pcl.convertLine("ID_REF\tIDENTIFIER\tGSM199453\tGSM199454\tGene title\tGene symbol");
		assertEquals("Platform reference identifier\tidentifier\tGWEIGHT\tGSM199453: Value for GSM199453: Pancreas at e12.5, Chip B, rep 1; src: Pancreas at e12.5\tGSM199454: Value for GSM199454: Pancreas at e12.5, Chip B, rep 2; src: Pancreas at e12.5\nEWEIGHT\t\t\t1\t1\n", line);
		assertFalse(soft2pcl.atLabels);
		assertTrue(soft2pcl.inDataSection);
		
		// Parse data lines
		line = soft2pcl.convertLine("104769_at\tLima1\t3.248\t3.334\tLIM domain and actin binding 1\tLima1");
		assertEquals("104769_at\tLima1\t1\t3.248\t3.334\n", line);
		assertFalse(soft2pcl.atLabels);
		assertTrue(soft2pcl.inDataSection);
		line = soft2pcl.convertLine("104770_at\tAI552899\t3.741\t3.953\t\t\n");
		assertEquals("104770_at\tAI552899\t1\t3.741\t3.953\n", line);
		// Invalid data line
		assertNull(soft2pcl.convertLine("104770_at\tAI552899"));
		
		// Parse end of data table
		assertNull(soft2pcl.convertLine("!dataset_table_end"));
		assertFalse(soft2pcl.atLabels);
		assertFalse(soft2pcl.inDataSection);
	}
	
	// Invalid header lines
	@Test(expected=ParseException.class)
	public void testConvertLines2() throws ParseException {
		GeoGDS2Pcl soft2pcl= new GeoGDS2Pcl();
		
		// Parse meta tags
		assertNull(soft2pcl.convertLine("^DATABASE = Geo"));
		assertNull(soft2pcl.convertLine("!dataset_reference_series = GSE8070"));
		assertNull(soft2pcl.convertLine("!dataset_sample_count = 2"));
		assertEquals(2, soft2pcl.nSamples);
	
		// Parse labels
		assertNull(soft2pcl.convertLine("#ID_REF = Platform reference identifier"));
		assertNull(soft2pcl.convertLine("#IDENTIFIER = identifier"));
		assertNull(soft2pcl.convertLine("#GSM199453 = Value for GSM199453: Pancreas at e12.5, Chip B, rep 1; src: Pancreas at e12.5"));
		assertNull(soft2pcl.convertLine("#GSM199454 = Value for GSM199454: Pancreas at e12.5, Chip B, rep 2; src: Pancreas at e12.5"));		
		
		assertNull(soft2pcl.convertLine("!dataset_table_begin"));		
		soft2pcl.convertLine("ID_REF\tIDENTIFIER");
	}
	
	// Header line with missing labels
	@Test(expected=ParseException.class)
	public void testConvertLines3() throws ParseException {
		GeoGDS2Pcl soft2pcl= new GeoGDS2Pcl();
		
		// Parse meta tags
		assertNull(soft2pcl.convertLine("^DATABASE = Geo"));
		assertNull(soft2pcl.convertLine("!dataset_reference_series = GSE8070"));
		assertNull(soft2pcl.convertLine("!dataset_sample_count = 2"));
		assertEquals(2, soft2pcl.nSamples);
	
		// Parse labels
		assertNull(soft2pcl.convertLine("#ID_REF = Platform reference identifier"));
		assertNull(soft2pcl.convertLine("#IDENTIFIER = identifier"));
		assertNull(soft2pcl.convertLine("#GSM199453 = Value for GSM199453: Pancreas at e12.5, Chip B, rep 1; src: Pancreas at e12.5"));
		assertNull(soft2pcl.convertLine("#GSM199454 = Value for GSM199454: Pancreas at e12.5, Chip B, rep 2; src: Pancreas at e12.5"));		
		
		assertNull(soft2pcl.convertLine("!dataset_table_begin"));		
		String line = soft2pcl.convertLine("ID_REF\t\tGSM199453\tGSM199454\tGene title\tGene symbol");
		assertEquals("ID_REF\tIDENTIFIER\tGWEIGHT\tGSM199453\tGSM199454\nEWEIGHT\t\t\t1\t1\n", line);
	}
	
	// Header line with missing labels 2
	@Test(expected=ParseException.class)
	public void testConvertLines4() throws ParseException {
		GeoGDS2Pcl soft2pcl= new GeoGDS2Pcl();
		
		// Parse meta tags
		assertNull(soft2pcl.convertLine("^DATABASE = Geo"));
		assertNull(soft2pcl.convertLine("!dataset_reference_series = GSE8070"));
		assertNull(soft2pcl.convertLine("!dataset_sample_count = 2"));
		assertEquals(2, soft2pcl.nSamples);
	
		// Parse labels
		assertNull(soft2pcl.convertLine("#ID_REF = Platform reference identifier"));
		assertNull(soft2pcl.convertLine("#IDENTIFIER = identifier"));
		assertNull(soft2pcl.convertLine("#GSM199453 = Value for GSM199453: Pancreas at e12.5, Chip B, rep 1; src: Pancreas at e12.5"));
		assertNull(soft2pcl.convertLine("#GSM199454 = Value for GSM199454: Pancreas at e12.5, Chip B, rep 2; src: Pancreas at e12.5"));		
		
		assertNull(soft2pcl.convertLine("!dataset_table_begin"));		
		String line = soft2pcl.convertLine("ID_REF\tIDENTIFIER\tGSM19\tGSM199454\tGene title\tGene symbol");
		assertEquals("ID_REF\tIDENTIFIER\tGWEIGHT\tGSM199453\tGSM199454\nEWEIGHT\t\t\t1\t1\n", line);
	}
	
	// Header line with missing labels 3
	@Test(expected=ParseException.class)
	public void testConvertLines5() throws ParseException {
		GeoGDS2Pcl soft2pcl= new GeoGDS2Pcl();
		
		// Parse meta tags
		assertNull(soft2pcl.convertLine("^DATABASE = Geo"));
		assertNull(soft2pcl.convertLine("!dataset_reference_series = GSE8070"));
		assertNull(soft2pcl.convertLine("!dataset_sample_count = 2"));
		assertEquals(2, soft2pcl.nSamples);
	
		// Parse labels
		assertNull(soft2pcl.convertLine("#ID_REF = Platform reference identifier"));
		assertNull(soft2pcl.convertLine("#IDENTIFIER = identifier"));
		assertNull(soft2pcl.convertLine("#GSM199453 = Value for GSM199453: Pancreas at e12.5, Chip B, rep 1; src: Pancreas at e12.5"));
		assertNull(soft2pcl.convertLine("#GSM199454 = Value for GSM199454: Pancreas at e12.5, Chip B, rep 2; src: Pancreas at e12.5"));		
		
		assertNull(soft2pcl.convertLine("!dataset_table_begin"));		
		String line = soft2pcl.convertLine("ID_REF\tIDENTIFIER\t\tGSM199454\tGene title\tGene symbol");
		assertEquals("ID_REF\tIDENTIFIER\tGWEIGHT\tGSM199453\tGSM199454\nEWEIGHT\t\t\t1\t1\n", line);
	}
	
	@Test
	public void testReset() throws ParseException {
		GeoGDS2Pcl soft2pcl= new GeoGDS2Pcl();
		
		assertNull(soft2pcl.convertLine("^DATABASE = Geo"));
		assertNull(soft2pcl.convertLine("!dataset_reference_series = GSE8070"));
		assertNull(soft2pcl.convertLine("!dataset_sample_count = 2"));
		assertEquals(2, soft2pcl.nSamples);
	
		assertNull(soft2pcl.convertLine("#ID_REF = Platform reference identifier"));
		assertNull(soft2pcl.convertLine("#IDENTIFIER = identifier"));
		assertNull(soft2pcl.convertLine("#GSM199453 = Value for GSM199453: Pancreas at e12.5, Chip B, rep 1; src: Pancreas at e12.5"));
		assertNull(soft2pcl.convertLine("#GSM199454 = Value for GSM199454: Pancreas at e12.5, Chip B, rep 2; src: Pancreas at e12.5"));
		
		assertNull(soft2pcl.convertLine("!dataset_table_begin"));
		assertTrue(soft2pcl.atLabels);
		assertFalse(soft2pcl.inDataSection);
		
		soft2pcl.reset();
		assertFalse(soft2pcl.atLabels);
		assertFalse(soft2pcl.inDataSection);
		assertTrue(soft2pcl.labels.isEmpty());		
	}

	@Test
	public void testConvert() throws IOException, ParseException {
		GeoGDS2Pcl soft2pcl = new GeoGDS2Pcl();
		
		BufferedReader ins = new BufferedReader(new FileReader(inputFilename));
		String outputFilename = OsPath.join(tmpDir, "GDS2924.pcl");
		BufferedWriter os = new BufferedWriter(new FileWriter(outputFilename));
		
		soft2pcl.convert(ins, os);
		
		ins.close();
		os.close();
		
		String[] lines = FSUtils.readTextFile(outputFilename);
		assertEquals(12479, lines.length);
		String[] headerCols = lines[0].split("\t");
		int nCols = soft2pcl.nSamples + 3;
		assertEquals(nCols, headerCols.length);
		assertEquals("Platform reference identifier", headerCols[0]);
		assertEquals("identifier", headerCols[1]);
		assertEquals("GWEIGHT", headerCols[2]);
		assertEquals("GSM199453: Value for GSM199453: Pancreas at e12.5, Chip B, rep 1; src: Pancreas at e12.5", headerCols[3]);
		assertEquals("GSM199462: Value for GSM199462: Pancreas at e16.5, Chip B, rep 2; src: Pancreas at e16.5", headerCols[12]);
		
		String[] weightCols = lines[1].split("\t");
		assertEquals(nCols, weightCols.length);
		assertEquals("EWEIGHT", weightCols[0]);
		assertEquals("", weightCols[1]);
		assertEquals("", weightCols[2]);
		for (int i = 3; i < nCols; i++) {
			assertEquals("1", weightCols[i]);
		}
		
		String[] firstRowCols = lines[3].split("\t");
		assertEquals(nCols, firstRowCols.length);
		assertEquals("104770_at", firstRowCols[0]);
		assertEquals("AI552899", firstRowCols[1]);
		assertEquals("1", firstRowCols[2]);
		assertEquals("3.741", firstRowCols[3]);
		assertEquals("3.953", firstRowCols[4]);
		assertEquals("4.200", firstRowCols[12]);
		for (int i = 2; i < lines.length - 1; i++) {
			String l = lines[i] + "\n";
			String[] cols = l.split("\t");
			if (nCols != cols.length) {
				System.err.println("Fail");
				fail(lines[i]);
			}
		}
		String lastLine = lines[lines.length - 1] + "\n";
		String[] lastRowCols = lastLine.split("\t");
		assertEquals(nCols, lastRowCols.length);
		assertEquals("AFFX-YEL024w/RIP1_at", lastRowCols[0]);
		assertEquals("--Control", lastRowCols[1]);
		assertEquals("1", lastRowCols[2]);
		assertEquals("", lastRowCols[3]);
		assertEquals("", lastRowCols[4]);
		assertEquals("\n", lastRowCols[12]);
	}
}
