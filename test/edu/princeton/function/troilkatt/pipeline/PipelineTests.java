package edu.princeton.function.troilkatt.pipeline;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ 
	StageTest.class,
	StageFactoryTest.class,
	NullStageTest.class,
	ExecuteDirTest.class, 
	ExecutePerFileTest.class,
	FilterTest.class, 
	FindGSMOverlapTest.class, 
	MapReduceStageTest.class,
	MapReduceTest.class, 	
	SaveFilelistTest.class,
	ScriptPerDirTest.class,	
	ScriptPerFileTest.class,
	SGEStageTest.class})
public class PipelineTests {

}
