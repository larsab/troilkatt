package edu.princeton.function.troilkatt.source;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ 
	SourceTest.class,
	SourceFactoryTest.class,
	NullSourceTest.class,
	ExecuteSourceTest.class, 	
	FileSourceTest.class,
	TFSSourceTest.class,
	ListDirTest.class, 	
	ListDirDiffTest.class,
	ListDirNewTest.class,
	GeoGDSMirrorTest.class, 
	GeoGSEMirrorTest.class,
	GeoRawMirrorTest.class,
	GeoRawOrgTest.class, 
	HbaseSourceTest.class,
	HREFSourceTest.class, 		
	ScriptSourceTest.class,
	OsCmdsSourceTest.class
	})
public class SourceTests {
	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main("edu.princeton.function.troilkatt.source.SourceTests");
	}
}
