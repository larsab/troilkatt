package edu.princeton.function.troilkatt.sink;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ 
	SinkTest.class,
	SinkFactoryTest.class,
	NullSinkTest.class,
	CopyToLocalFSTest.class, 
	CopyToRemoteTest.class,
	GlobalMetaTest.class})
public class SinkTests {
	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main("edu.princeton.function.troilkatt.sink.SinkTests");
	}
}
