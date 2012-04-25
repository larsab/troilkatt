package edu.princeton.function.troilkatt.tools;

import static org.junit.Assert.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.princeton.function.troilkatt.TestSuper;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.tools.GeoGSMOverlap.Duplicate;
import edu.princeton.function.troilkatt.tools.GeoGSMOverlap.OverlapSet;
import edu.princeton.function.troilkatt.tools.GeoGSMOverlap.Superset;

public class GeoGSMOverlapTest extends TestSuper {
	protected static Logger testLogger;
	public static String[] lines;
	public static final String metaData = "\t2011\tFoo\t2010\tBar"; // ignored

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		
		testLogger = Logger.getLogger("test");
		
		lines = new String[15];
		
		lines[0] = "GSE13142\tGSE13143\t15,15,15\t" + createSamples("13143", 1, 15) + metaData; // [1-15]
		lines[1] = "GSE13143\tGSE13143-1\t7,15,7\t" + createSamples("13143", 1, 7) + metaData;  // [1-7]
		lines[2] = "GSE13143\tGSE13143-2\t3,15,3\t" + createSamples("13143", 8, 3) + metaData;  // [8-10]
		lines[3] = "GSE13143\tGSE13143-3\t5,15,5\t" + createSamples("13143", 11, 5) + metaData; // [10-15]
		lines[12] = "GSE13142\tGSE13143-1\t7,15,7\t" + createSamples("13143", 1, 7) + metaData;  // [1-7]
		lines[13] = "GSE13142\tGSE13143-2\t3,15,3\t" + createSamples("13143", 8, 3) + metaData;  // [8-10]
		lines[14] = "GSE13142\tGSE13143-3\t5,15,5\t" + createSamples("13143", 11, 5) + metaData; // [10-15]
		
		lines[4] = "GSE13\tGSE14\t5,20,5\t" + createSamples("13", 1, 5) + metaData; // [1-5]
		lines[5] = "GSE13\tGSE15\t3,20,3\t" + createSamples("13", 4, 3) + metaData; // [4-6]
		lines[6] = "GSE13\tGSE16\t8,20,10\t" + createSamples("13", 4, 8) + metaData;// [4-11]
		lines[7] = "GSE16\tGSE17\t8,10,8\t" + createSamples("13", 4, 8) + metaData; // [4-11]
		lines[8] = "GSE16\tGSE18\t8,10,10\t" + createSamples("13", 4, 8) + metaData;// [4-11]
		
		lines[9] =  "GSE107\tGSE108\t4,10,6\t" + createSamples("107", 1, 4) + metaData;  // [1-4]
		lines[10] = "GSE107\tGSE109\t1,10,4\t" + createSamples("107", 8, 1) + metaData; // [8]
		lines[11] = "GSE108\tGSE109\t1,6,4\t" + createSamples("107", 8, 1) + metaData;  // [1]
	}

	private static String createSamples(String prefix, int f, int n) {
		String gsms = "GSM" + prefix + String.valueOf(f);
		for (int i = f + 1; i < f + n; i++) {
			gsms = gsms + ",GSM" + prefix + String.valueOf(i);
		}
		return gsms;
	}
	
	private static String[] createSampleList(String prefix, int f, int n) {
		String[] gsms = new String[n];
		for (int i = f; i < f + n; i++) {
			gsms[i - f] = "GSM" + prefix + String.valueOf(i);
		}
		return gsms;
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
	public void testFindGSMOverlap() {
		GeoGSMOverlap parser = new GeoGSMOverlap();
		assertNotNull(parser.duplicates);
		assertNotNull(parser.overlap);
		assertNotNull(parser.subsetLinks);
		assertNotNull(parser.supersets);
		assertNotNull(parser.treeRoots);
		assertNotNull(parser.minSamplesRemoved);
		assertNotNull(parser.clusters);
		
	}
	
	// Tests for Duplicate class
	@Test
	public void testDuplicate() {
		GeoGSMOverlap parser = new GeoGSMOverlap();
		String[] gsms = {"GSM1", "GSM2", "GSM3"};
		GeoGSMOverlap.Duplicate dup = parser.new Duplicate("GSE1", "GSE2-2", gsms);
		
		assertEquals("GSE1", dup.gid1);
		assertEquals("GSE2-2", dup.gid2);
		assertEquals(3, dup.gsms.length);
		assertEquals("GSM2", dup.gsms[1]);
		assertEquals(3, dup.getNOverlapping());
	}
	
	// Tests for Superset class
	@Test
	public void testSuperset() {
		GeoGSMOverlap parser = new GeoGSMOverlap();
		String[] gids = {"GSE2", "GSE3"};
		GeoGSMOverlap.Superset sup = parser.new Superset("GSE1", 5, gids);
		
		assertEquals("GSE1", sup.gid);
		assertEquals(5, sup.nSamples);
		assertEquals(2, sup.subsetIDs.length);
		assertEquals("GSE3", sup.subsetIDs[1]);
	}

	// Test for OverlapSet and OverlapLink classes
	@Test
	public void testOverlapSetLink() {
		GeoGSMOverlap parser = new GeoGSMOverlap();
		GeoGSMOverlap.OverlapSet os1 = parser.new OverlapSet("GSE1", 3); // GSM1, GSM2, GSM3
		assertEquals("GSE1", os1.gid);
		assertEquals(3, os1.nSamples);
		assertNotNull(os1.subsets);
		assertNotNull(os1.gsmsToRemove);
		assertEquals(-1, os1.clusterID);
		GeoGSMOverlap.OverlapSet os2 = parser.new OverlapSet("GSE2", 2); // GSM3, GSM4
		GeoGSMOverlap.OverlapSet os3 = parser.new OverlapSet("GSE3", 4); // GSM1, GSM3, GSM5, GSM6
		
		String[] o1t2 = {"GSM3"};
		String[] o1t3 = {"GSM1", "GSM3"};
		String[] o2t3 = {"GSM3"};		
		
		GeoGSMOverlap.OverlapLink l1t2 = parser.new OverlapLink(os1, os2, o1t2);
		assertEquals(os1, l1t2.link1);
		assertEquals(os2, l1t2.link2);
		assertEquals(1, l1t2.gsms.length);
		assertEquals("GSM3", l1t2.gsms[0]);	
		assertEquals(1, l1t2.getNOverlapping());
		GeoGSMOverlap.OverlapLink l1t3 = parser.new OverlapLink(os1, os3, o1t3);
		GeoGSMOverlap.OverlapLink l2t3 = parser.new OverlapLink(os2, os3, o2t3);
		
		assertEquals(2, os1.getNOverlapSamples());
		assertEquals(1, os2.getNOverlapSamples());
		assertEquals(2, os3.getNOverlapSamples());
		
		assertEquals(6, os1.getNSubSamples());
		assertEquals(7, os2.getNSubSamples());
		assertEquals(5, os3.getNSubSamples());
		
		assertFalse(os1.isLeaf());
		assertFalse(os2.isLeaf());
		assertFalse(os3.isLeaf());
		
		String[] gids = os1.getSubsetGids();
		assertEquals(2, gids.length);
		assertEquals("GSE2", gids[0]);
		assertEquals("GSE3", gids[1]);
		
		// Remove some of the links
		l1t3.unlink();
		l1t3.unlink(); // should not do anything
		assertEquals(os2, l1t2.getOther(os1));
		l1t2.getOther(os1).subsets.remove(l1t2);
		assertEquals(os3, l2t3.getOther(os2));
		l2t3.getOther(os2).subsets.remove(l2t3);
		
		assertEquals(1, os1.getNOverlapSamples());
		assertEquals(1, os2.getNOverlapSamples());
		assertEquals(0, os3.getNOverlapSamples());
		
		assertFalse(os1.isLeaf());
		assertFalse(os2.isLeaf());
		assertTrue(os3.isLeaf());
	
		gids = os1.getSubsetGids();
		assertEquals(1, gids.length);
		assertEquals("GSE2", gids[0]);
		gids = os2.getSubsetGids();
		assertEquals(1, gids.length);
		assertEquals("GSE3", gids[0]);
		gids = os3.getSubsetGids();
		assertEquals(0, gids.length);
	}
	
	// Test for OverlapSet and OverlapLink classes
	@Test(expected=RuntimeException.class)
	public void testOverlapSetLink2() {
		GeoGSMOverlap parser = new GeoGSMOverlap();
		GeoGSMOverlap.OverlapSet os1 = parser.new OverlapSet("GSE1", 3); // GSM1, GSM2, GSM3
		GeoGSMOverlap.OverlapSet os2 = parser.new OverlapSet("GSE2", 3); // GSM3, GSM4
		GeoGSMOverlap.OverlapSet os3 = parser.new OverlapSet("GSE3", 4); // GSM1, GSM3, GSM5, GSM6
		
		String[] o1t2 = {"GSM3"};
		GeoGSMOverlap.OverlapLink l1t2 = parser.new OverlapLink(os1, os2, o1t2);
		l1t2.getOther(os3);
	}
	
	@Test
	public void testAddOverlapLine() throws ParseException {
		GeoGSMOverlap parser = new GeoGSMOverlap();
		
		for (String l: lines) {
			assertTrue(parser.addOverlapLine(l));
		}
		
		// Invalid lines
		//String line = "GSE13144\tGSE13143\t15,15,15\t" + createSamples("13143", 1, 15) + metaData;
		//assertFalse(parser.addOverlapLine(line));
		String line = "GDS2027\tGSE13143\t15,15,15\t" + createSamples("13143", 1, 15) + metaData;
		assertFalse(parser.addOverlapLine(line));
		
		assertEquals(0, parser.duplicates.size());		
		assertEquals(14, parser.overlap.size());
		assertTrue(parser.overlap.containsKey("GSE13143"));
		assertTrue(parser.overlap.containsKey("GSE13143-2"));
		assertTrue(parser.overlap.containsKey("GSE13"));
		assertTrue(parser.overlap.containsKey("GSE109"));
		assertEquals(15, parser.subsetLinks.size());
		
		assertEquals(15, parser.overlap.get("GSE13142").getNOverlapSamples());
		assertEquals(15, parser.overlap.get("GSE13143").getNOverlapSamples());
		assertEquals(11, parser.overlap.get("GSE13").getNOverlapSamples());
		assertEquals(5, parser.overlap.get("GSE107").getNOverlapSamples());
		assertEquals(7, parser.overlap.get("GSE13143-1").getNOverlapSamples());
		assertEquals(8, parser.overlap.get("GSE16").getNOverlapSamples());
		assertEquals(1, parser.overlap.get("GSE109").getNOverlapSamples());
	}
	
	@Test(expected=ParseException.class)
	public void testAddOverlapLine2() throws ParseException {
		GeoGSMOverlap parser = new GeoGSMOverlap();
		
		for (String l: lines) {
			assertTrue(parser.addOverlapLine(l));
		}
		
		// Invalid lines
		String line = "GSE13142\tGSE13143\t15,15,15\t" + createSamples("13143", 1, 15) + "\tfoo";
		assertFalse(parser.addOverlapLine(line));
		
	}
	
	@Test(expected=ParseException.class)
	public void testAddOverlapLine3() throws ParseException {
		GeoGSMOverlap parser = new GeoGSMOverlap();
		
		for (String l: lines) {
			assertTrue(parser.addOverlapLine(l));
		}
		
		// Invalid lines
		String line = "GSE13142\tGSE13143\t15,15,15\t" + createSamples("13143", 1, 15) + metaData + " \ttoomuch";
		assertFalse(parser.addOverlapLine(line));
	}
	
	@Test(expected=ParseException.class)
	public void testAddOverlapLine4() throws ParseException {
		GeoGSMOverlap parser = new GeoGSMOverlap();
		
		for (String l: lines) {
			assertTrue(parser.addOverlapLine(l));
		}
		
		// Invalid lines
		String line = "GSE13142\tGSE13143\t15,15\t" + createSamples("13143", 1, 15) + metaData;
		assertFalse(parser.addOverlapLine(line));
	}
	
	@Test(expected=ParseException.class)
	public void testAddOverlapLine5() throws ParseException {
		GeoGSMOverlap parser = new GeoGSMOverlap();
		
		for (String l: lines) {
			assertTrue(parser.addOverlapLine(l));
		}
		
		// Invalid lines
		String line = "GSE13142\tGSE13143\t15,15,15,15\t" + createSamples("13143", 1, 15) + metaData;
		assertFalse(parser.addOverlapLine(line));
	}
	
	@Test(expected=ParseException.class)
	public void testAddOverlapLine6() throws ParseException {
		GeoGSMOverlap parser = new GeoGSMOverlap();
		
		for (String l: lines) {
			assertTrue(parser.addOverlapLine(l));
		}
		
		// Invalid lines
		String line = "GSE13142\tGSE13143\tfoo,15,15\t" + createSamples("13143", 1, 15) + metaData;
		assertFalse(parser.addOverlapLine(line));
	}
	
	@Test(expected=ParseException.class)
	public void testAddOverlapLine7() throws ParseException {
		GeoGSMOverlap parser = new GeoGSMOverlap();
		
		for (String l: lines) {
			assertTrue(parser.addOverlapLine(l));
		}
		
		// Invalid lines
		String line = "GSE13142\tGSE13143\t15,foo,15\t" + createSamples("13143", 1, 15) + metaData;
		assertFalse(parser.addOverlapLine(line));
		line = "GSE13142\tGSE13143\t15,15,foo\t" + createSamples("13143", 1, 15) + metaData;
		assertFalse(parser.addOverlapLine(line));
		line = "GSE13142\tGSE13143\t15,15,15\t" + metaData;
		assertFalse(parser.addOverlapLine(line));
		
	}
	
	@Test(expected=ParseException.class)
	public void testAddOverlapLine8() throws ParseException {
		GeoGSMOverlap parser = new GeoGSMOverlap();
		
		for (String l: lines) {
			assertTrue(parser.addOverlapLine(l));
		}
		
		// Invalid lines
		String line = "GSE13142\tGSE13143\t15,15,foo\t" + createSamples("13143", 1, 15) + metaData;
		assertFalse(parser.addOverlapLine(line));		
	}
	
	@Test(expected=ParseException.class)
	public void testAddOverlapLine9() throws ParseException {
		GeoGSMOverlap parser = new GeoGSMOverlap();
		
		for (String l: lines) {
			assertTrue(parser.addOverlapLine(l));
		}
		
		// Invalid lines
		String line = "GSE13142\tGSE13143\t15,15,15\t\t" + metaData;
		assertFalse(parser.addOverlapLine(line));
		
	}

	@Test
	public void testRemoveDuplicates() throws ParseException {
		GeoGSMOverlap parser = new GeoGSMOverlap();		
		for (String l: lines) {
			assertTrue(parser.addOverlapLine(l));
		}
		
		ArrayList<String> removed = parser.removeDuplicates();
		assertEquals(1, removed.size());
		assertEquals("GSE13142", removed.get(0));
	}

	@Test
	public void testRemoveSmallSets() throws ParseException {
		GeoGSMOverlap parser = new GeoGSMOverlap();		
		for (String l: lines) {
			assertTrue(parser.addOverlapLine(l));
		}
		ArrayList<String> removed = parser.removeDuplicates();
		assertEquals(1, removed.size());
		
		removed = parser.removeSmallSets(3);
		assertTrue(removed.isEmpty());
		assertEquals(13, parser.overlap.size());
		assertEquals(11, parser.subsetLinks.size());
		assertEquals(0, parser.minSamplesRemoved.size());

		removed = parser.removeSmallSets(4);
		assertEquals(2, removed.size());
		assertTrue(removed.contains("GSE13143-2"));
		assertTrue(removed.contains("GSE15"));
		assertEquals(2, parser.minSamplesRemoved.size());
		OverlapSet o1 = parser.minSamplesRemoved.get(0);
		OverlapSet o2 = parser.minSamplesRemoved.get(1);
		assertTrue(o1.gid.equals("GSE13143-2") || o1.gid.equals("GSE15"));
		assertTrue(o2.gid.equals("GSE13143-2") || o2.gid.equals("GSE15"));
		
		assertTrue(removed.contains("GSE13143-2"));
		assertTrue(removed.contains("GSE15"));

		assertEquals(11, parser.overlap.size());
		assertEquals(9, parser.subsetLinks.size());
		
		assertEquals(12, parser.overlap.get("GSE13143").getNOverlapSamples());
		assertEquals(11, parser.overlap.get("GSE13").getNOverlapSamples());
		assertEquals(5, parser.overlap.get("GSE107").getNOverlapSamples());
		assertEquals(7, parser.overlap.get("GSE13143-1").getNOverlapSamples());
		assertEquals(8, parser.overlap.get("GSE16").getNOverlapSamples());
		assertEquals(1, parser.overlap.get("GSE109").getNOverlapSamples());
	}

	@Test
	public void testRemoveSupersets() throws ParseException {
		GeoGSMOverlap parser = new GeoGSMOverlap();		
		for (String l: lines) {
			assertTrue(parser.addOverlapLine(l));
		}
		
		ArrayList<String> removed = parser.removeDuplicates();
		removed = parser.removeSmallSets(3);
		assertTrue(removed.isEmpty());
		assertEquals(13, parser.overlap.size());
		assertEquals(11, parser.subsetLinks.size());
		
		removed = parser.removeSupersets();
		assertEquals(1, removed.size());
		assertEquals("GSE13143", removed.get(0));
		assertEquals(12, parser.overlap.size());
		assertEquals(8, parser.subsetLinks.size());	
		
		assertEquals(0, parser.overlap.get("GSE13143-1").getNOverlapSamples());
		assertEquals(11, parser.overlap.get("GSE13").getNOverlapSamples());
		assertEquals(5, parser.overlap.get("GSE107").getNOverlapSamples());		
		assertEquals(8, parser.overlap.get("GSE16").getNOverlapSamples());
		assertEquals(1, parser.overlap.get("GSE109").getNOverlapSamples());
	}

	@Test
	public void testOrderSets() throws ParseException {
		GeoGSMOverlap parser = new GeoGSMOverlap();		
		for (String l: lines) {
			assertTrue(parser.addOverlapLine(l));
		}
		
		assertEquals(15, parser.overlap.get("GSE13143").getNOverlapSamples());
		assertEquals(11, parser.overlap.get("GSE13").getNOverlapSamples());
		assertEquals(5, parser.overlap.get("GSE107").getNOverlapSamples());
		assertEquals(7, parser.overlap.get("GSE13143-1").getNOverlapSamples());
		assertEquals(8, parser.overlap.get("GSE16").getNOverlapSamples());
		assertEquals(1, parser.overlap.get("GSE109").getNOverlapSamples());
		
		ArrayList<String> removed = parser.removeDuplicates();
		removed = parser.removeSmallSets(3);
		assertTrue(removed.isEmpty());
		assertEquals(13, parser.overlap.size());
		assertEquals(11, parser.subsetLinks.size());
		assertEquals(15, parser.overlap.get("GSE13143").getNOverlapSamples());
		assertEquals(11, parser.overlap.get("GSE13").getNOverlapSamples());
		assertEquals(5, parser.overlap.get("GSE107").getNOverlapSamples());
		assertEquals(7, parser.overlap.get("GSE13143-1").getNOverlapSamples());
		assertEquals(8, parser.overlap.get("GSE16").getNOverlapSamples());
		assertEquals(1, parser.overlap.get("GSE109").getNOverlapSamples());
		
		removed = parser.removeSupersets();
		assertEquals(1, removed.size());
		assertEquals("GSE13143", removed.get(0));
		assertEquals(12, parser.overlap.size());
		assertEquals(8, parser.subsetLinks.size());		
		assertEquals(11, parser.overlap.get("GSE13").getNOverlapSamples());
		assertEquals(5, parser.overlap.get("GSE107").getNOverlapSamples());
		assertEquals(0, parser.overlap.get("GSE13143-1").getNOverlapSamples());
		assertEquals(8, parser.overlap.get("GSE16").getNOverlapSamples());
		assertEquals(1, parser.overlap.get("GSE109").getNOverlapSamples());
		
		parser.orderSets();
		assertEquals(5, parser.overlap.get("GSE107").getNOverlapSamples());
		assertEquals(1, parser.overlap.get("GSE108").getNOverlapSamples());
		assertEquals(0, parser.overlap.get("GSE109").getNOverlapSamples());
		assertEquals(0, parser.overlap.get("GSE13143-1").getNOverlapSamples());
		assertEquals(11, parser.overlap.get("GSE13").getNOverlapSamples());
		assertEquals(5, parser.overlap.get("GSE107").getNOverlapSamples());
		
		assertEquals(6, parser.treeRoots.size());
		assertTrue(parser.treeRoots.contains(parser.overlap.get("GSE13143-1")));
		assertTrue(parser.treeRoots.contains(parser.overlap.get("GSE13143-2")));
		assertTrue(parser.treeRoots.contains(parser.overlap.get("GSE13143-3")));
		assertTrue(parser.treeRoots.contains(parser.overlap.get("GSE13")));
		assertTrue(parser.treeRoots.contains(parser.overlap.get("GSE18")));
		assertTrue(parser.treeRoots.contains(parser.overlap.get("GSE107")));
	}

	@Test
	public void testReduceOverlap() throws ParseException {
		GeoGSMOverlap parser = new GeoGSMOverlap();		
		for (String l: lines) {
			assertTrue(parser.addOverlapLine(l));
		}
		
		ArrayList<String> removed = parser.removeDuplicates();
		removed = parser.removeSmallSets(3);
		assertTrue(removed.isEmpty());
		assertEquals(13, parser.overlap.size());
		assertEquals(11, parser.subsetLinks.size());
		
		removed = parser.removeSupersets();
		assertEquals(1, removed.size());
		assertEquals("GSE13143", removed.get(0));
		assertEquals(12, parser.overlap.size());
		assertEquals(8, parser.subsetLinks.size());
		
		removed = parser.reduceOverlap(3, 3);
		assertEquals(1, parser.overlap.get("GSE107").getNOverlapSamples());
		assertEquals(1, parser.overlap.get("GSE108").getNOverlapSamples());
		assertEquals(0, parser.overlap.get("GSE109").getNOverlapSamples());		
		assertEquals(2, removed.size());
		assertTrue(removed.contains("GSE16"));
		assertTrue(removed.contains("GSE18"));
		assertEquals(2, parser.minSamplesRemoved.size());
		OverlapSet o1 = parser.minSamplesRemoved.get(0);
		OverlapSet o2 = parser.minSamplesRemoved.get(1);
		assertTrue(o1.gid.equals("GSE16") || o1.gid.equals("GSE18"));
		assertTrue(o2.gid.equals("GSE16") || o2.gid.equals("GSE18"));
		assertEquals(10, parser.overlap.size());
		assertEquals(7, parser.subsetLinks.size());
		assertEquals(9, parser.treeRoots.size());
		assertTrue(parser.treeRoots.contains(parser.overlap.get("GSE17")));
		
		assertEquals(11, parser.overlap.get("GSE13").gsmsToRemove.size());
		assertEquals(4, parser.overlap.get("GSE107").gsmsToRemove.size());
		assertEquals(0, parser.overlap.get("GSE108").gsmsToRemove.size());
		assertEquals(0, parser.overlap.get("GSE14").gsmsToRemove.size());
		assertEquals(0, parser.overlap.get("GSE13143-1").gsmsToRemove.size());		
	}

	@Test
	public void testGetClusters() throws ParseException {
		GeoGSMOverlap parser = new GeoGSMOverlap();		
		for (String l: lines) {
			assertTrue(parser.addOverlapLine(l));
		}
		parser.findClusters();
		ArrayList<ArrayList<String>> clusterIDs = parser.getClusterIDs(1);
		assertEquals(3, clusterIDs.size());
		
		for (int i = 0; i < 3; i++) {
			ArrayList<String> clids = clusterIDs.get(i);
			if (clids.contains("GSE13143")) {
				assertEquals(5, clids.size());
				assertTrue(clids.contains("GSE13142"));
				assertTrue(clids.contains("GSE13143"));
				assertTrue(clids.contains("GSE13143-1"));
				assertTrue(clids.contains("GSE13143-2"));
				assertTrue(clids.contains("GSE13143-3"));
			}
			else if (clids.contains("GSE13")) {
				assertEquals(6, clids.size());
				assertTrue(clids.contains("GSE14"));
				assertTrue(clids.contains("GSE15"));
				assertTrue(clids.contains("GSE18"));
			}
			else if (clids.contains("GSE108")) {
				assertEquals(3, clids.size());
				assertTrue(clids.contains("GSE108"));
				assertTrue(clids.contains("GSE109"));
				assertTrue(clids.contains("GSE107"));
			}
			else {
				fail("Invalid clsuter");
			}
		}
		
		ArrayList<ArrayList<OverlapSet>> clusters = parser.getClusters(1);
		assertEquals(3, clusters.size());
		
		parser.find(3, 3);
		parser.findClusters();
		
		clusterIDs = parser.getClusterIDs(1);
		assertEquals(8, clusterIDs.size());
		for (int i = 0; i < clusterIDs.size(); i++) {
			ArrayList<String> clids = clusterIDs.get(i);
			if (clids.contains("GSE13143-1")) {
				assertEquals(1, clids.size());
			}
			else if (clids.contains("GSE13143-2")) {
				assertEquals(1, clids.size());
			}
			else if (clids.contains("GSE13143-3")) {
				assertEquals(1, clids.size());
			}
			else if (clids.contains("GSE13")) {
				assertEquals(1, clids.size());				
			}
			else if (clids.contains("GSE14")) {
				assertEquals(1, clids.size());				
			}
			else if (clids.contains("GSE15")) {
				assertEquals(1, clids.size());				
			}
			else if (clids.contains("GSE108")) {
				assertEquals(3, clids.size());
				assertTrue(clids.contains("GSE108"));
				assertTrue(clids.contains("GSE109"));
				assertTrue(clids.contains("GSE107"));
			}
			else if (clids.contains("GSE17")) {
				assertEquals(1, clids.size());				
			}
			else {
				fail("Invalid clsuter");
			}
		}
		
		clusterIDs = parser.getClusterIDs(2);
		assertEquals(1, clusterIDs.size());		
		ArrayList<String> clids = clusterIDs.get(0);
		assertEquals(3, clids.size());
		assertTrue(clids.contains("GSE108"));
		assertTrue(clids.contains("GSE109"));
		assertTrue(clids.contains("GSE107"));
			
		clusters = parser.getClusters(2);
		assertEquals(1, clusters.size());
		ArrayList<OverlapSet> cls = clusters.get(0);			
		int found = 0;
		for (OverlapSet cl: cls) {
			if (cl.gid.equals("GSE107")) {
				found = found + 1;
			}
			else if (cl.gid.equals("GSE108")) {
				found = found + 10;
			}
			else if (cl.gid.equals("GSE109")) {
				found = found + 100;
			}
			else {
				fail("Invalid cluster: " + cl.gid);
			}					
		}
		assertEquals(111, found);
	}

	@Test
	public void testGetDuplicateIDs() throws ParseException {
		GeoGSMOverlap parser = new GeoGSMOverlap();		
		for (String l: lines) {
			assertTrue(parser.addOverlapLine(l));
		}
		parser.removeDuplicates();
		
		HashSet<String> gids = parser.getDuplicateIDs();
		assertEquals(1, gids.size());
		assertTrue(gids.contains("GSE13142"));
	}

	@Test
	public void testGetDuplicates() throws ParseException {
		GeoGSMOverlap parser = new GeoGSMOverlap();		
		for (String l: lines) {
			assertTrue(parser.addOverlapLine(l));
		}
		parser.removeDuplicates();
		
		ArrayList<Duplicate> dups = parser.getDuplicates();
		assertEquals(1, dups.size());
		assertEquals("GSE13142", dups.get(0).gid1);
		assertEquals("GSE13143", dups.get(0).gid2);
		assertEquals(15, dups.get(0).gsms.length);
	}

	@Test
	public void testGetSupersetIDs() throws ParseException {
		GeoGSMOverlap parser = new GeoGSMOverlap();		
		for (String l: lines) {
			assertTrue(parser.addOverlapLine(l));
		}
		parser.removeDuplicates();
		parser.removeSmallSets(3);
		parser.removeSupersets();
		
		HashSet<String> gids = parser.getSupersetIDs();
		assertEquals(1, gids.size());
		assertTrue(gids.contains("GSE13143"));	
	}

	@Test
	public void testGetSupersets() throws ParseException {
		GeoGSMOverlap parser = new GeoGSMOverlap();		
		for (String l: lines) {
			assertTrue(parser.addOverlapLine(l));
		}
		parser.removeDuplicates();
		parser.removeSmallSets(3);
		parser.removeSupersets();
		
		ArrayList<Superset> sups = parser.getSupersets();
		assertEquals(1, sups.size());
		assertEquals("GSE13143", sups.get(0).gid);
	}

	@Test
	public void testGetRemovedIDs() throws ParseException {
		GeoGSMOverlap parser = new GeoGSMOverlap();		
		for (String l: lines) {
			assertTrue(parser.addOverlapLine(l));
		}
		parser.removeDuplicates();
		parser.removeSmallSets(3);
		parser.removeSupersets();		
		parser.reduceOverlap(3, 3);
		
		HashSet<String> removed = parser.getRemovedIDs();
		assertEquals(2, removed.size());
		assertTrue(removed.contains("GSE16"));
		assertTrue(removed.contains("GSE18"));
	}
	
	@Test
	public void testGetRemoved() throws ParseException {
		GeoGSMOverlap parser = new GeoGSMOverlap();		
		for (String l: lines) {
			assertTrue(parser.addOverlapLine(l));
		}
		parser.removeDuplicates();
		parser.removeSmallSets(3);
		parser.removeSupersets();
		parser.reduceOverlap(3, 3);
		
		ArrayList<OverlapSet> removed = parser.getRemoved();
		assertEquals(2, removed.size());
		OverlapSet o1 = removed.get(0);
		OverlapSet o2 = removed.get(1);
		assertTrue(o1.gid.equals("GSE16") || o1.gid.equals("GSE18"));
		assertTrue(o2.gid.equals("GSE16") || o2.gid.equals("GSE18"));
	}
	
	@Test
	public void testGetSamplesToRemove() throws ParseException {
		GeoGSMOverlap parser = new GeoGSMOverlap();		
		for (String l: lines) {
			assertTrue(parser.addOverlapLine(l));
		}
		parser.removeDuplicates();
		parser.removeSmallSets(3);
		parser.removeSupersets();
		parser.reduceOverlap(3, 3);
		
		HashMap<String, HashSet<String>> toRemove = parser.getRemovedSamples();
		assertEquals(2, toRemove.size());
		assertTrue(toRemove.containsKey("GSE13"));
		assertTrue(toRemove.containsKey("GSE107"));
		
		HashSet<String> gsms = toRemove.get("GSE13");
		assertEquals(11, gsms.size());
		for (String s: createSampleList("13", 1, 11)) { // GSE13-GSE14
			assertTrue(gsms.contains(s));
		}
		
		gsms = toRemove.get("GSE107");
		assertEquals(4, gsms.size());
		for (String s: createSampleList("107", 1, 4)) { //GSE107-GSE108
			assertTrue(gsms.contains(s));
		}
	}
	
	@Test
	public void testCompareIDs() {		
		assertTrue(GeoGSMOverlap.compareIDs("GDS100", "GDS1001") < 0);
		assertTrue(GeoGSMOverlap.compareIDs("GDS69", "GDS100") < 0);
		assertTrue(GeoGSMOverlap.compareIDs("GDS1001", "GDS100") > 0);
		assertTrue(GeoGSMOverlap.compareIDs("GDS100", "GDS100") == 0);
		assertTrue(GeoGSMOverlap.compareIDs("GDS100", "GDS100-1") == 0);
		assertTrue(GeoGSMOverlap.compareIDs("GDS100-1", "GDS100-2") == 0);
		assertTrue(GeoGSMOverlap.compareIDs("GDS100-4", "GDS1001") < 0);
		assertTrue(GeoGSMOverlap.compareIDs("GDS100-4", "GDS100-4") == 0);
		assertTrue(GeoGSMOverlap.compareIDs("GDS100-4", "GDS100-3") == 0);
	}
	
	@Test
	public void testFind() throws ParseException, IOException {
		doFind();
		
		String filename = OsPath.join(tmpDir, "overlap");
		assertTrue(OsPath.isfile(filename));
		
		String logFilename = OsPath.join(tmpDir, "overlap.log");
		assertTrue(OsPath.isfile(logFilename));
		
		// File content is not tested since this is tested in testReadOverlapFile
	}
	
	private GeoGSMOverlap doFind() throws IOException, ParseException {
		GeoGSMOverlap parser = new GeoGSMOverlap();		
		for (String l: lines) {
			assertTrue(parser.addOverlapLine(l));
		}
		
		BufferedWriter outputFile = new BufferedWriter(new FileWriter(OsPath.join(tmpDir, "overlap")));
		BufferedWriter logFile = new BufferedWriter(new FileWriter(OsPath.join(tmpDir, "overlap.log")));
		parser.find(outputFile, logFile, 3, 3);		
		outputFile.close();
		logFile.close();
		
		return parser;
	}
	
	@Test
	public void testReadOverlapFile() throws IOException, ParseException {
		// Create overlap file
		doFind();
		
		ArrayList<String> deletedDatasets = new ArrayList<String>();
		String filename = OsPath.join(tmpDir, "overlap");
		HashMap<String, String[]> deletedSamples = new HashMap<String, String[]>();
		GeoGSMOverlap.readOverlapFile(filename, deletedDatasets, deletedSamples, testLogger);
				
		assertEquals(4, deletedDatasets.size());
		// Duplicates
		assertTrue(deletedDatasets.contains("GSE13142"));		
		// Supersets
		assertTrue(deletedDatasets.contains("GSE13143"));		
		// Too small
		assertTrue(deletedDatasets.contains("GSE16"));
		assertTrue(deletedDatasets.contains("GSE18"));
		
		// Removed samples
		assertEquals(2, deletedSamples.size());
		assertTrue(deletedSamples.containsKey("GSE13"));
		assertTrue(deletedSamples.containsKey("GSE107"));
		
		String[] gsms = deletedSamples.get("GSE13");
		assertEquals(11, gsms.length);
		Arrays.sort(gsms);
		for (String s: createSampleList("13", 1, 11)) { // GSE13-GSE14
			assertTrue(Arrays.binarySearch(gsms, s) >= 0);
		}
		
		gsms = deletedSamples.get("GSE107");
		assertEquals(4, gsms.length);
		Arrays.sort(gsms);
		for (String s: createSampleList("107", 1, 4)) { //GSE107-GSE108			
			assertTrue(Arrays.binarySearch(gsms, s) >= 0);
		}
	}
	
	// Invalid filename
	@Test(expected=IOException.class)
	public void testReadOverlapFile2() throws IOException, ParseException {
		// Create overlap file
		doFind();
		
		ArrayList<String> deletedDatasets = new ArrayList<String>();
		String filename = OsPath.join(tmpDir, "non-existing-file");
		HashMap<String, String[]> deletedSamples = new HashMap<String, String[]>();
		GeoGSMOverlap.readOverlapFile(filename, deletedDatasets, deletedSamples, testLogger);
	}
	
	// Invalid file
	@Test(expected=IOException.class)
	public void testReadOverlapFile3() throws IOException, ParseException {
		// Create overlap file
		doFind();
		
		ArrayList<String> deletedDatasets = new ArrayList<String>();
		String filename = OsPath.join(tmpDir, "overlap.log");
		HashMap<String, String[]> deletedSamples = new HashMap<String, String[]>();
		GeoGSMOverlap.readOverlapFile(filename, deletedDatasets, deletedSamples, testLogger);
	}

	@Test
	public void testGetDeleteColumnIndexes() throws IOException, ParseException {
		// Create overlap file
		doFind();
				
		// Read overlap file
		ArrayList<String> deletedDatasets = new ArrayList<String>();
		String filename = OsPath.join(tmpDir, "overlap");
		HashMap<String, String[]> deletedSamples = new HashMap<String, String[]>();
		GeoGSMOverlap.readOverlapFile(filename, deletedDatasets, deletedSamples, testLogger);
		
		// Create header line: 10 samples, first 4 overlap: starts at GSM1071
		String headerLine = "ID\tName\tEWEIGHT\tGSM1071: 1st sample\tGSM1072: 2nd sample\tGSM1073: 3rd sample\tGSM1074: 4th sample\tGSM1075: 5th sample\tGSM1076: 6th sample\tGSM1077: 7th sample\tGSM1078: 8th sample\tGSM1079: 9th sample\tGSM10710: 10th sample\n";
			 
		String[] toDelete = deletedSamples.get("GSE107");
		assertNotNull(toDelete);
		assertEquals(4, toDelete.length);
		Arrays.sort(toDelete);
		assertEquals("GSM1071", toDelete[0]);
		assertEquals("GSM1072", toDelete[1]);
		assertEquals("GSM1073", toDelete[2]);
		assertEquals("GSM1074", toDelete[3]);
			
		// Parse first line to find indexes of samples to delete
		int[] deleteColumnIndexes = GeoGSMOverlap.getDeleteColumnIndexes(toDelete, headerLine);
		assertEquals(4, deleteColumnIndexes.length);
		Arrays.sort(deleteColumnIndexes);
		assertEquals(3, deleteColumnIndexes[0]);
		assertEquals(4, deleteColumnIndexes[1]);
		assertEquals(5, deleteColumnIndexes[2]);
		assertEquals(6, deleteColumnIndexes[3]);
	}
	
	// Too few columns
	@Test
	public void testGetDeleteColumnIndexes2() throws IOException, ParseException {
		// Create overlap file
		doFind();
				
		// Read overlap file
		ArrayList<String> deletedDatasets = new ArrayList<String>();
		String filename = OsPath.join(tmpDir, "overlap");
		HashMap<String, String[]> deletedSamples = new HashMap<String, String[]>();
		GeoGSMOverlap.readOverlapFile(filename, deletedDatasets, deletedSamples, testLogger);
		
		// Invalid header line
		String headerLine = "ID\tName\tEWEIGHT\n";
			 
		String[] toDelete = deletedSamples.get("GSE107");
		assertNotNull(toDelete);
		assertEquals(4, toDelete.length);
			
		// Parse first line to find indexes of samples to delete
		int[] deleteColumnIndexes = GeoGSMOverlap.getDeleteColumnIndexes(toDelete, headerLine);
		assertNull(deleteColumnIndexes);
	}		
	
	// Not all samples in header line
	@Test
	public void testGetDeleteColumnIndexes3() throws IOException, ParseException {
		// Create overlap file
		doFind();
				
		// Read overlap file
		ArrayList<String> deletedDatasets = new ArrayList<String>();
		String filename = OsPath.join(tmpDir, "overlap");
		HashMap<String, String[]> deletedSamples = new HashMap<String, String[]>();
		GeoGSMOverlap.readOverlapFile(filename, deletedDatasets, deletedSamples, testLogger);
		
		// Invalid header line
		String headerLine = "ID\tName\tEWEIGHT\tGSM1071: 1st sample\tGSM1073: 3rd sample\tGSM1075: 5th sample\tGSM1076: 6th sample\tGSM1077: 7th sample\tGSM1078: 8th sample\tGSM1071: 10th sample\n";
			 
		String[] toDelete = deletedSamples.get("GSE107");
		assertNotNull(toDelete);
		
		// Parse first line to find indexes of samples to delete
		int[] deleteColumnIndexes = GeoGSMOverlap.getDeleteColumnIndexes(toDelete, headerLine);
		assertNull(deleteColumnIndexes);
	}	
	
	@Test
	public void testDeleteColumnsFromLine() throws IOException, ParseException {
		// Create overlap file
		doFind();
						
		// Read overlap file
		ArrayList<String> deletedDatasets = new ArrayList<String>();
		String filename = OsPath.join(tmpDir, "overlap");
		HashMap<String, String[]> deletedSamples = new HashMap<String, String[]>();
		GeoGSMOverlap.readOverlapFile(filename, deletedDatasets, deletedSamples, testLogger);
				
		// Get indexes of columns to delete
		String headerLine = "ID\tName\tEWEIGHT\tGSM1071: 1st sample\tGSM1072: 2nd sample\tGSM1073: 3rd sample\tGSM1074: 4th sample\tGSM1075: 5th sample\tGSM1076: 6th sample\tGSM1077: 7th sample\tGSM1078: 8th sample\tGSM1079: 9th sample\tGSM10710: 10th sample\n";					 
		String[] toDelete = deletedSamples.get("GSE107");
		assertEquals(4, toDelete.length);
		int[] deleteColumnIndexes = GeoGSMOverlap.getDeleteColumnIndexes(toDelete, headerLine);
		assertEquals(4, deleteColumnIndexes.length);
		
		assertEquals("ID\tName\tEWEIGHT\tGSM1075: 5th sample\tGSM1076: 6th sample\tGSM1077: 7th sample\tGSM1078: 8th sample\tGSM1079: 9th sample\tGSM10710: 10th sample\n",
				GeoGSMOverlap.deleteColumnsFromLine(headerLine, deleteColumnIndexes));
		
		String delLine = "i\tname\tw\t1\t2\t3\t4\t5\t6\t7\t8\t9\10\n";
		assertEquals("i\tname\tw\t5\t6\t7\t8\t9\10\n",
				GeoGSMOverlap.deleteColumnsFromLine(delLine, deleteColumnIndexes));
	}
}
