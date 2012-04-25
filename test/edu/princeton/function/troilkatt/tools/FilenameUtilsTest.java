package edu.princeton.function.troilkatt.tools;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class FilenameUtilsTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
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
	public void testGetDsetID() {
		assertEquals("GDS101", FilenameUtils.getDsetID("GDS101.pcl.1001.gz"));
		assertEquals("GDS101", FilenameUtils.getDsetID("GDS101_full.pcl.1001.gz"));
		assertEquals("GDS101-part1", FilenameUtils.getDsetID("GDS101-part1.pcl.1001.gz"));
		assertEquals("GDS101", FilenameUtils.getDsetID("GDS101.pcl"));
		assertNull(FilenameUtils.getDsetID(null));
		
		assertEquals("GSE101", FilenameUtils.getDsetID("/foo/bar/GSE101.pcl.1001.gz"));
		assertEquals("GSE101", FilenameUtils.getDsetID("foo/bar/GSE101_family.pcl.1001.gz"));
		assertEquals("GSE101-part1", FilenameUtils.getDsetID("foo-bar-baz/GSE101-part1.pcl.1001.gz"));
		assertEquals("GSE101", FilenameUtils.getDsetID("foo_baz/GSE101.pcl"));
		assertNull(FilenameUtils.getDsetID(null));
	}

	@Test
	public void testGetDsetID2() {
		assertEquals("GDS101", FilenameUtils.getDsetID("GDS101.pcl.1001.gz", true));
		assertEquals("GDS101", FilenameUtils.getDsetID("GDS101.pcl.1001.gz", false));
		assertEquals("GDS101", FilenameUtils.getDsetID("GDS101_full.pcl.1001.gz", true));
		assertEquals("GDS101", FilenameUtils.getDsetID("GDS101_full.pcl.1001.gz", false));
		assertEquals("GDS101-part1", FilenameUtils.getDsetID("GDS101-part1.pcl.1001.gz", true));
		assertEquals("GDS101", FilenameUtils.getDsetID("GDS101-part1.pcl.1001.gz", false));
		assertEquals("GDS101", FilenameUtils.getDsetID("GDS101.pcl", true));
		assertEquals("GDS101", FilenameUtils.getDsetID("GDS101.pcl", false));
		assertNull(FilenameUtils.getDsetID(null));
		
		assertEquals("GDS101", FilenameUtils.getDsetID("/foo/bar/GDS101.pcl.1001.gz", true));
		assertEquals("GDS101", FilenameUtils.getDsetID("/foo/bar/GDS101.pcl.1001.gz", false));
		assertEquals("GDS101", FilenameUtils.getDsetID("foo/bar/GDS101_full.pcl.1001.gz", true));
		assertEquals("GDS101", FilenameUtils.getDsetID("foo/bar/GDS101_full.pcl.1001.gz", false));
		assertEquals("GDS101-part1", FilenameUtils.getDsetID("foo-bar-baz/GDS101-part1.pcl.1001.gz", true));
		assertEquals("GDS101", FilenameUtils.getDsetID("foo-bar-baz/GDS101-part1.pcl.1001.gz", false));
		assertEquals("GDS101", FilenameUtils.getDsetID("foo_baz/GDS101.pcl", true));
		assertEquals("GDS101", FilenameUtils.getDsetID("foo_baz/GDS101.pcl", false));
		assertNull(FilenameUtils.getDsetID(null));
		
		assertEquals("GSE101", FilenameUtils.getDsetID("GSE101.pcl.1001.gz", true));
		assertEquals("GSE101", FilenameUtils.getDsetID("GSE101.pcl.1001.gz", false));
		assertEquals("GSE101", FilenameUtils.getDsetID("GSE101_family.pcl.1001.gz", true));
		assertEquals("GSE101", FilenameUtils.getDsetID("GSE101_family.pcl.1001.gz", false));
		assertEquals("GSE101-part1", FilenameUtils.getDsetID("GSE101-part1.pcl.1001.gz", true));
		assertEquals("GSE101", FilenameUtils.getDsetID("GSE101-part1.pcl.1001.gz", false));
		assertEquals("GSE101", FilenameUtils.getDsetID("GSE101.pcl", true));
		assertEquals("GSE101", FilenameUtils.getDsetID("GSE101.pcl", false));
		assertNull(FilenameUtils.getDsetID(null));
		
		assertEquals("GSE101", FilenameUtils.getDsetID("/foo/bar/GSE101.pcl.1001.gz", true));
		assertEquals("GSE101", FilenameUtils.getDsetID("/foo/bar/GSE101.pcl.1001.gz", false));
		assertEquals("GSE101", FilenameUtils.getDsetID("foo/bar/GSE101_family.pcl.1001.gz", true));
		assertEquals("GSE101", FilenameUtils.getDsetID("foo/bar/GSE101_family.pcl.1001.gz", false));
		assertEquals("GSE101-part1", FilenameUtils.getDsetID("foo-bar-baz/GSE101-part1.pcl.1001.gz", true));
		assertEquals("GSE101", FilenameUtils.getDsetID("foo-bar-baz/GSE101-part1.pcl.1001.gz", false));
		assertEquals("GSE101", FilenameUtils.getDsetID("foo_baz/GSE101.pcl", true));
		assertEquals("GSE101", FilenameUtils.getDsetID("foo_baz/GSE101.pcl", false));
		assertNull(FilenameUtils.getDsetID(null));
	}

	@Test
	public void testMergeDsetPlatIDs() {
		assertEquals("GDS101-GPL82", FilenameUtils.mergeDsetPlatIDs("GDS101", "GPL82"));
		assertEquals("GSE101-GPL82", FilenameUtils.mergeDsetPlatIDs("GSE101", "GPL82"));
	}

	@Test
	public void testGetPlatID() {
		assertEquals("GPL82", FilenameUtils.getPlatID("GDS101-GPL82"));
		assertEquals("GPL82", FilenameUtils.getPlatID("GDS101-GPL82.pcl"));
		assertEquals("GPL82", FilenameUtils.getPlatID("GDS101-GPL82.pcl.mv.1001.gz"));
		assertEquals("GPL82", FilenameUtils.getPlatID("GDS101-GPL82.pcl-mv-1.101.bz2"));
		assertEquals("GPL82", FilenameUtils.getPlatID("/foo-bar/baz/GDS101-GPL82.pcl.101.bz2"));
		
		assertEquals("", FilenameUtils.getPlatID("GDS101"));
		assertEquals("", FilenameUtils.getPlatID("GDS101.pcl"));
		assertEquals("", FilenameUtils.getPlatID("GDS101pcl.mv.1001.gz"));
		assertEquals("", FilenameUtils.getPlatID("GDS101.pcl-mv-1.101.bz2"));
		assertEquals("", FilenameUtils.getPlatID("/foo-bar/baz/GDS101.pcl.101.bz2"));
		
		assertNull(FilenameUtils.getPlatID(null));
	}

}
