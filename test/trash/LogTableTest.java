package trash;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.princeton.function.troilkatt.hbase.LogTableSchema;

public class LogTableTest {

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
	public void testLogTable() {
		LogTableSchema t = new LogTableSchema("testLog");
		assertEquals("testLog", t.tableName);
		assertEquals(4, t.colFams.length);
		assertEquals("out", t.colFams[0]);
		assertEquals("error", t.colFams[1]);
		assertEquals("log", t.colFams[2]);
		assertEquals("other", t.colFams[3]);
	}	
}
