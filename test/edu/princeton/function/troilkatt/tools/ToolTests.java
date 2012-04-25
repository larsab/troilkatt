package edu.princeton.function.troilkatt.tools;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ 
	FilenameUtilsTest.class, 
	GeoGDS2PclTest.class,
	GeoGDSParserTest.class, 
	GeoGSE2PclTest.class, 
	GeoGSEParserTest.class,
	GeoGSMOverlapTest.class, 
	Pcl2InfoTest.class })
public class ToolTests {

}
