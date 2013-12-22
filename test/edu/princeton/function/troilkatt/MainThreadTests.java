package edu.princeton.function.troilkatt;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ TroilkattPropertiesTest.class,
	TroilkattStatusTest.class, 
	TroilkattTest.class, 
	PipelineTest.class,
	PipelinePlaceholderTest.class})
public class MainThreadTests {
	public static void main(String args[]) {
		org.junit.runner.JUnitCore.main("edu.princeton.function.troilkatt.MainThreadTests");
	}
}
