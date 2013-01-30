package edu.princeton.function.troilkatt.fs;

import static org.junit.Assert.*;

import java.io.IOException;
import org.apache.log4j.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.princeton.function.troilkatt.PipelineException;
import edu.princeton.function.troilkatt.TestSuper;

/**
 * 
 * NOTE! Many of the test fails when run "at full speed". This is due to clearTable() function used 
 * to clear a table. One soultion is to step through the tests in debug mode
 */
public class LogTableTest extends TestSuper {
	static final protected String tableName = "troilkatt-log-unitLogTable";	
	protected static Logger testLogger;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {		
		testLogger = Logger.getLogger("test");
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
	public void testLogTable() throws PipelineException, IOException {		
		LogTable logTable = new LogTable("unitLogTable");
		assertEquals(tableName, logTable.tableName);		
		assertNotNull(logTable.logger);		
	}

}
