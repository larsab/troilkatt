package edu.princeton.function.troilkatt.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class LogTableTest {
	static final protected String tableName = "unitTable";
	static protected Configuration hbConf;
	static protected HBaseAdmin hbAdm;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		hbConf = HBaseConfiguration.create();
		hbAdm = new HBaseAdmin(hbConf);
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

	// Tested as part of pipeline.MapReduceTest.testProcess2()
	//@Test
	//public void testPutMapReduceLogFiles() {
	//}
	
	// Tested as part of pipeline.MapReduceTest.testProcess2()
	//@Test
	//public void testGetMapReduceLogFiles() {
	//}
}
