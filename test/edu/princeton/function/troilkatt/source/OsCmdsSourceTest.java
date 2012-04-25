package edu.princeton.function.troilkatt.source;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.Troilkatt;
import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFS;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class OsCmdsSourceTest extends edu.princeton.function.troilkatt.TestSuper {
	protected static TroilkattProperties troilkattProperties;				
	protected static TroilkattFS tfs;
	protected static Pipeline pipeline;
	protected static Logger testLogger;
	protected static String[] cmds;
	
	protected static String localRootDir;
	protected static String hdfsStageMetaDir;
	protected static String hdfsStageTmpDir;
	
	protected static String tmpFilename;
	protected static String cmdsFile;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		OsPath.mkdir(tmpDir);
		cmdsFile = OsPath.join(tmpDir, "cmds");
		createCmdsFile(cmdsFile);
		
		testLogger = Logger.getLogger("test");
		
		troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));		
		FileSystem hdfs = FileSystem.get(new Configuration());			
		tfs = new TroilkattFS(hdfs);
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs);
		
		localRootDir = tmpDir;
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"), OsPath.join("meta", pipeline.name));
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 0, "executeSource"));
		hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"), "tmp");	
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		OsPath.delete(tmpFilename);
		OsPath.delete(tmpFilename + ".wc");
	}

	@After
	public void tearDown() throws Exception {
		OsPath.delete(tmpFilename);
		OsPath.delete(tmpFilename + ".wc");
	}
	
	public static void createCmdsFile(String fname) throws IOException {
		tmpFilename = "TROILKATT.TMP_DIR/OsCmds.tmp";
		cmds = new String[6];
		cmds[0] = "echo 'foo bar baz' > " + tmpFilename;
		cmds[1] = "cp " + tmpFilename + " " + tmpFilename + ".copy";
		cmds[2] = "# A comment";
		cmds[3] = ""; // and a blank line
		cmds[4] = "wc " + tmpFilename + " > " + tmpFilename + ".wc";
		cmds[5] = "rm " + tmpFilename + ".copy";
				
		FSUtils.writeTextFile(fname, cmds);
	}

	@Test
	public void testOsCmdsSource() throws TroilkattPropertiesException, StageInitException {
		OsCmdsSource source = new OsCmdsSource("osCmdsSource", cmdsFile,
				"test/osCmdsSource", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertNotNull(source);		
		assertNotNull(source.cmds);
		assertEquals(4, source.cmds.length);
		assertTrue(source.cmds[0].startsWith("echo"));
		assertFalse(source.cmds[0].contains("TROILKATT.TMP_DIR"));		
	}
	
	// Invalid filename
	@Test(expected=StageInitException.class)
	public void testOsCmdsSource2() throws TroilkattPropertiesException, StageInitException {
		OsCmdsSource source = new OsCmdsSource("osCmdsSource", "/invalid/filename",
				"test/osCmdsSource", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		assertNotNull(source);
	}

	@Test
	public void testRetrieve() throws TroilkattPropertiesException, StageInitException, IOException, StageException {
		OsCmdsSource source = new OsCmdsSource("osCmdsSource", cmdsFile,
				"test/osCmdsSource", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		ArrayList<String> metaFiles = new ArrayList<String>();		
		ArrayList<String> logFiles = new ArrayList<String>();
		source.retrieve(metaFiles, logFiles, 168);
				
		String lines[] = FSUtils.readTextFile(OsPath.join(source.stageTmpDir, "OsCmds.tmp"));
		assertEquals("foo bar baz", lines[0]);
		
		assertTrue(OsPath.isfile(OsPath.join(source.stageTmpDir, "OsCmds.tmp.wc")));
	}
}
