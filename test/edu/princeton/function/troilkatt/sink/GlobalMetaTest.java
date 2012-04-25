package edu.princeton.function.troilkatt.sink;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TestSuper;
import edu.princeton.function.troilkatt.Troilkatt;
import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFS;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class GlobalMetaTest extends TestSuper {
	protected GlobalMeta sink;	
	protected static String outputDir;
	protected static Logger testLogger;
	protected static Pipeline pipeline;
	protected static TroilkattFS tfs;
	
	protected static String localRootDir;
	protected static String hdfsStageMetaDir;
	protected static String hdfsStageTmpDir;

	protected static String globalMetaDir;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		outputDir = OsPath.join(tmpDir, "sink");
		testLogger = Logger.getLogger("test");
		
		TroilkattProperties troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));
		Configuration hdfsConfig = new Configuration();
		FileSystem hdfs = FileSystem.get(hdfsConfig);			
		tfs = new TroilkattFS(hdfs);
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs);
		
		localRootDir = tmpDir;		
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"), OsPath.join("meta", pipeline.name));
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 3, "unitSink"));
		hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"), "tmp");
		
		globalMetaDir = troilkattProperties.get("troilkatt.globalfs.global-meta.dir");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {		
		
	}

	@Test
	public void testSink() throws TroilkattPropertiesException, StageInitException, StageException, IOException {
		OsPath.delete(OsPath.join(globalMetaDir, "file1"));
		OsPath.delete(OsPath.join(globalMetaDir, "file2"));
		OsPath.delete(OsPath.join(globalMetaDir, "subdir2/file6"));
		assertFalse(OsPath.isfile(OsPath.join(globalMetaDir, "file1")));
		assertFalse(OsPath.isfile(OsPath.join(globalMetaDir, "file2")));
        assertFalse(OsPath.isfile(OsPath.join(globalMetaDir, "subdir2/file6")));
		sink = new GlobalMeta(3, "unitGlobalSink", null,	
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
		
		ArrayList<String> inputFiles = new ArrayList<String>();
		ArrayList<String> metaFiles = new ArrayList<String>();
		ArrayList<String> logFiles = new ArrayList<String>();
		
		tfs.mkdir(OsPath.join(hdfsRoot, "sink"));
		inputFiles.add(OsPath.join(hdfsRoot, "sink/file1.1.none"));
		inputFiles.add(OsPath.join(hdfsRoot, "sink/file2.2.none"));
		inputFiles.add(OsPath.join(hdfsRoot, "sink/subdir2/file6.6.none"));
		
		Path srcFile = new Path(OsPath.join(dataDir, "files/file1"));
		for (String f: inputFiles) {
			tfs.hdfs.copyFromLocalFile(srcFile, new Path(f));
		}
		
		sink.sink(inputFiles, metaFiles, logFiles, 1201);
        
		assertTrue(OsPath.isfile(OsPath.join(globalMetaDir, "file1")));
        assertTrue(OsPath.isfile(OsPath.join(globalMetaDir, "file2")));
        assertTrue(OsPath.isfile(OsPath.join(globalMetaDir, "file6")));
        
        assertEquals(0, metaFiles.size());
        assertEquals(0, logFiles.size());
        
        // Rerun with empty arg
        sink = new GlobalMeta(3, "unitGlobalSink", "",	
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
        sink.sink(inputFiles, metaFiles, logFiles, 1202);
        
		assertTrue(OsPath.isfile(OsPath.join(globalMetaDir, "file1")));
        assertTrue(OsPath.isfile(OsPath.join(globalMetaDir, "file2")));
        assertTrue(OsPath.isfile(OsPath.join(globalMetaDir, "file6")));
        
        // Rerun with subdir
        sink = new GlobalMeta(3, "unitGlobalSink", "test",	
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
        sink.sink(inputFiles, metaFiles, logFiles, 1203);        
		assertTrue(OsPath.isfile(OsPath.join(globalMetaDir, "test/file1")));
        assertTrue(OsPath.isfile(OsPath.join(globalMetaDir, "test/file2")));
        assertTrue(OsPath.isfile(OsPath.join(globalMetaDir, "test/file6")));
	}
	
	
}
