package edu.princeton.function.troilkatt.mapreduce;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ 
	TroilkattMapReduceTest.class,
	GeneCounterTest.class, 
	PerFileTest.class,	
	ReCompressTest.class, 	
	RawRemoveOverlappingTest.class, 
	SplitSoftTest.class,
	
	// TODO: change tests such that dummy dataset IDs are used instead
	// Note! Make sure to change the table names in GeoMetaTableSchema and GSMTableSchema 
	//UpdateGEOMetaTableTest.class,
	//UpdateGSMTableTest.class,
	//GSMOverlapTest.class
	})
public class MapReduceTests {
	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main("edu.princeton.function.troilkatt.mapreduce.MapReduceTests");
	}
}
