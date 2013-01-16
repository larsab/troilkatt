package edu.princeton.function.troilkatt.fs;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ 
	OsPathTest.class, // coverage
	FSUtilsTest.class, // coverage
	HadoopSetupTest.class, // coverage
	TroilkattFSTest.class,
	TroilkattHDFSTest.class,
	TroilkattNFSTest.class,
	LogTableTest.class})
public class FSTests {

}
