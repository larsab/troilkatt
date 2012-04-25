package edu.princeton.function.troilkatt.fs;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ 
	OsPathTest.class, 
	FSUtilsTest.class, 
	HadoopSetupTest.class,
	TroilkattFSTest.class,
	LogTableTest.class})
public class FSTests {

}
