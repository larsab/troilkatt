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
	UpdateGEOMetaTableTest.class,
	UpdateGSMTableTest.class,
	GSMOverlapTest.class})
public class MapReduceTests {

}
