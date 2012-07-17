package edu.princeton.function.troilkatt.utils;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class UtilsTest {

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

	/**
	 * Test method for {@link edu.princeton.function.troilkatt.Troilkatt#getYesOrNo(java.lang.String, boolean)}.
	 */
	//@Test
	public void testGetYesOrNo() {
		// Yes'
		assertTrue(Utils.getYesOrNo("Enter 'y'", true));
		assertFalse(Utils.getYesOrNo("Enter 'n'", true));
		assertTrue(Utils.getYesOrNo("Enter 'Y'", true));		
		assertTrue(Utils.getYesOrNo("Enter 'yes'", true));
		assertTrue(Utils.getYesOrNo("Enter 'Yes'", true));
		assertTrue(Utils.getYesOrNo("Press Enter", true));
		
		// No's
		assertFalse(Utils.getYesOrNo("Enter 'n'", true));
		assertTrue(Utils.getYesOrNo("Enter 'Y'", true));
		assertFalse(Utils.getYesOrNo("Enter 'N'", true));		
		assertFalse(Utils.getYesOrNo("Enter 'no'", true));
		assertFalse(Utils.getYesOrNo("Enter 'No'", true));
		assertFalse(Utils.getYesOrNo("Press Enter", false));
	
		// Invalid input
		assertTrue(Utils.getYesOrNo("Enter 'foo' then 'y'", true));
		assertFalse(Utils.getYesOrNo("Enter 'foo', then 'bar', and finally press Enter", false));
	}
	
	@Test
	public void testMergeArrays() {
		String[] a1 = {"foo", "bar"};
		String[] a2 = {"baz"};
		String[] a3 = {"foo", "bar", "baz"};
		String[] a4 = {"baz", "foo", "bar"};
		String[] a5 = new String[0];
		
		String[] res = Utils.mergeArrays(a1, a2);
		assertTrue(Arrays.equals(res, a3));
		
		res = Utils.mergeArrays(a2, a1);
		assertTrue(Arrays.equals(res, a4));
		
		res = Utils.mergeArrays(a1, a5);
		assertTrue(Arrays.equals(res, a1));
		
		res = Utils.mergeArrays(a5, a2);
		assertTrue(Arrays.equals(res, a2));
		
		res = Utils.mergeArrays(a1, null);
		assertTrue(Arrays.equals(res, a1));
		
		res = Utils.mergeArrays(null, a2);
		assertTrue(Arrays.equals(res, a2));
		
		res = Utils.mergeArrays(null, null);
		assertNull(res);
	}

	@Test
	public void testArray2list() {
		String[] a = {"foo", "bar", "baz"};
		ArrayList<String> l = Utils.array2list(a);
		
		assertEquals(l.size(), a.length);
		for (int i = 0; i < l.size(); i++) {
			assertEquals(l.get(i), a[i]);
		}
		
		a = new String[0];
		l = Utils.array2list(a);
		assertEquals(0, l.size());
		
		assertNull(Utils.array2list(null));
	}

}
