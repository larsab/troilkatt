package edu.princeton.function.troilkatt.hbase;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ 
	TableTest.class, 
	TroilkattTableTest.class,
	GeoMetaTableTest.class})
public class HbaseTests {
	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main("edu.princeton.function.troilkatt.hbase.HbaseTests");
	}
}
