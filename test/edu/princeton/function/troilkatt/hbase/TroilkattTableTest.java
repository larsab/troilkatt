package edu.princeton.function.troilkatt.hbase;


import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TroilkattTableTest {

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
	public void testArray2String2Array() {
		ArrayList<String> a = new ArrayList<String>();
		
		a.add("foo");
		a.add("bar");
		a.add("baz\nbongo");
		
		String s = TroilkattTable.array2string(a);
		assertEquals("foo\nbar\nbaz<NEWLINE>bongo", s);
		
		ArrayList<String> b = TroilkattTable.string2array(s);
		
		assertEquals(a.size(), b.size());
		assertEquals("foo", b.get(0));
		assertEquals("bar", b.get(1));
		assertEquals("baz\nbongo", b.get(2));
		
		a.clear();
		s = TroilkattTable.array2string(a);
		assertEquals("", s);
		b = TroilkattTable.string2array(s);
		assertEquals(0, b.size());
		
		s = TroilkattTable.array2string(null);
		assertNull(s);
		b = TroilkattTable.string2array(null);
		assertNull(b);
	}
}
