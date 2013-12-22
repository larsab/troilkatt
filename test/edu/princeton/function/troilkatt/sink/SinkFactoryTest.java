package edu.princeton.function.troilkatt.sink;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.PipelineException;
import edu.princeton.function.troilkatt.TestSuper;
import edu.princeton.function.troilkatt.Troilkatt;
import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.LogTableHbase;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class SinkFactoryTest extends TestSuper {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testNewSink() throws IOException, TroilkattPropertiesException, StageInitException, PipelineException {		
		TroilkattProperties troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));
		Configuration hdfsConfig = new Configuration();
		FileSystem hdfs = FileSystem.get(hdfsConfig);			
		TroilkattHDFS tfs = new TroilkattHDFS(hdfs);
		LogTableHbase lt = new LogTableHbase("unitPipeline", HBaseConfiguration.create());
		Pipeline pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs, lt);
		Logger testLogger = Logger.getLogger("testLogger");
		
		Sink sink = SinkFactory.newSink("copy_to_local", 2, "sinkTest",
				"/tmp/", pipeline, testLogger);
		assertEquals(CopyToLocalFS.class, sink.getClass());
		
		sink = SinkFactory.newSink("copy_to_remote", 3, "sinkTest",
				"/tmp/", pipeline, testLogger);
		assertEquals(CopyToRemote.class, sink.getClass());
		
		sink = SinkFactory.newSink("null_sink", 2, "sinkTest",
				"/tmp/", pipeline, testLogger);
		assertEquals(NullSink.class, sink.getClass());
		
		sink = SinkFactory.newSink("global_meta_sink", 2, "sinkTest",
				"/tmp/", pipeline, testLogger);
		assertEquals(GlobalMeta.class, sink.getClass());
	}

}
