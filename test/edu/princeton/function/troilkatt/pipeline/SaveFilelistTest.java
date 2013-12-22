package edu.princeton.function.troilkatt.pipeline;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TestSuper;
import edu.princeton.function.troilkatt.Troilkatt;
import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.LogTableHbase;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;

public class SaveFilelistTest extends TestSuper {
	protected static TroilkattProperties troilkattProperties;				
	protected static TroilkattHDFS tfs;
	protected static Pipeline pipeline;
	protected static LogTableHbase lt;
	
	protected static Logger testLogger;
	
	protected static String localRootDir;
	protected static String hdfsStageMetaDir;
	protected static String hdfsStageTmpDir;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		testLogger = Logger.getLogger("test");
		
		troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));		
		FileSystem hdfs = FileSystem.get(new Configuration());			
		tfs = new TroilkattHDFS(hdfs);
		lt = new LogTableHbase("unitPipeline", HBaseConfiguration.create());
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs, lt);
		
		localRootDir = tmpDir;		
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), OsPath.join("meta", pipeline.name));
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 5, "null"));
		hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "tmp");
	}
	
	@Test
	public void testSaveFilelist() throws TroilkattPropertiesException, StageInitException {
		String filename = OsPath.join(tmpDir, "filelist/filelist.txt");
		SaveFilelist stage = new SaveFilelist(5, "filelist", filename,
				null, null, 0, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		assertEquals(filename, stage.listFilename);
		assertTrue(OsPath.isdir(OsPath.join(tmpDir, "filelist")));
	}
	
	// Invalid directory
	@Test(expected=StageInitException.class)
	public void testSaveFilelist2() throws TroilkattPropertiesException, StageInitException {
		String filename ="/root/filelist/filelist.txt";
		SaveFilelist stage = new SaveFilelist(5, "filelist", filename,
				null, null, 0, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		assertEquals(filename, stage.listFilename);		
	}

	@Test
	public void testProcess2() throws TroilkattPropertiesException, StageInitException, StageException, IOException {
		String filename = OsPath.join(tmpDir, "filelist/filelist.txt");
		SaveFilelist stage = new SaveFilelist(5, "filelist", filename,
				null, null, 0, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		ArrayList<String> inputFiles = new ArrayList<String>();		
		inputFiles.add("foo");
		inputFiles.add("bar");
		inputFiles.add("baz");
						
		ArrayList<String> outputFiles = stage.process2(inputFiles, 6521334);
		
		assertEquals(inputFiles.size(), outputFiles.size());
		for (int i = 0; i < inputFiles.size(); i++) {
			assertEquals(inputFiles.get(i), outputFiles.get(i));
		}
		
		String[] lines = FSUtils.readTextFile(filename);
		assertEquals(inputFiles.size(), lines.length);
		for (int i = 0; i < inputFiles.size(); i++) {
			assertEquals(inputFiles.get(i), lines[i]);
		}
	}
}
