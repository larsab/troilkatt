package edu.princeton.function.troilkatt;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Before;
import org.junit.Test;

import edu.princeton.function.troilkatt.fs.LogTableHbase;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFS;
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;

public class PipelinePlaceholderTest extends TestSuper {
	private String pipelineName = "pipelinePlaceholderTest";
	protected TroilkattProperties troilkattProperties;
	protected static TroilkattFS tfs;
	protected LogTableHbase lt;
	
	@Before
	public void setUp() throws Exception {		
		troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));		
		tfs = new TroilkattHDFS(new Configuration());
		lt = new LogTableHbase(pipelineName, HBaseConfiguration.create());
	}

	@Test
	public void testPipelinePlaceholder() throws PipelineException, TroilkattPropertiesException {		
		PipelinePlaceholder p = new PipelinePlaceholder(pipelineName, troilkattProperties, 
				tfs, lt);
		assertNotNull(p);
		assertEquals(pipelineName, p.name);
		assertEquals(troilkattProperties, p.troilkattProperties);
		assertEquals(tfs, p.tfs);
		assertNotNull(p.logTable);		
		String localRoot = troilkattProperties.get("troilkatt.localfs.dir");
		assertTrue(OsPath.isdir(OsPath.join(localRoot, pipelineName)));
	}
	
	@Test(expected=RuntimeException.class)
	public void testUpdate() throws PipelineException, TroilkattPropertiesException, IOException {
		PipelinePlaceholder p = new PipelinePlaceholder(pipelineName, troilkattProperties, 
				tfs, lt);
		TroilkattStatus status = new TroilkattStatus(tfs, troilkattProperties);
		p.update(10, status);
	}

	@Test(expected=RuntimeException.class)
	public void testRecover() throws PipelineException, TroilkattPropertiesException, IOException {
		PipelinePlaceholder p = new PipelinePlaceholder(pipelineName, troilkattProperties, 
				tfs, lt);
		TroilkattStatus status = new TroilkattStatus(tfs, troilkattProperties);
		p.recover(11, status);
	}

	

}
