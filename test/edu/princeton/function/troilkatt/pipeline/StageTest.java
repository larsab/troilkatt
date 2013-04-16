package edu.princeton.function.troilkatt.pipeline;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattHDFS;
import edu.princeton.function.troilkatt.hbase.HbaseException;

public class StageTest extends TestSuper {
		
	protected static TroilkattProperties troilkattProperties;
	protected static TroilkattHDFS tfs;
	protected static Pipeline pipeline;	
	protected static String hdfsRoot;
	protected static Logger testLogger;
	
	protected static int stageNum = 3;
	protected static String stageName = "unitStage";
	protected static String localRootDir;
	protected static String hdfsStageMetaDir;
	protected static String hdfsStageTmpDir;
	
	protected Stage stage;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		testLogger = Logger.getLogger("test");
		TestSuper.initTestDir();
		
		Configuration hdfsConfig = new Configuration();
		FileSystem hdfs = FileSystem.get(hdfsConfig);			
		tfs = new TroilkattHDFS(hdfs);
		
		troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));	
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs);
					
		localRootDir = tmpDir;
		hdfsRoot = troilkattProperties.get("troilkatt.tfs.root.dir");
		String hdfsPipelineMetaDir = OsPath.join(hdfsRoot, OsPath.join("meta", pipeline.name));
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", stageNum, stageName));
		hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "tmp");			
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		OsPath.deleteAll(tmpDir);
		OsPath.deleteAll(outDir);		
				
		stage = new Stage(stageNum, stageName, "foo bar baz",
				"test/stage", "bz2", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);
	}

	@After
	public void tearDown() throws Exception {
		OsPath.deleteAll(tmpDir);
		OsPath.deleteAll(outDir);
		// No cleanup since stage specific directories are in tmpDir which is deleted above
	}

	@Test
	public void testConstructor() throws TroilkattPropertiesException, StageInitException {				
		assertEquals("003-unitStage", stage.stageName);
		assertEquals("foo bar baz", stage.args);
		assertTrue(stage.tfsOutputDir.endsWith("test/stage"));		
		assertEquals("bz2", stage.compressionFormat);
		assertEquals(10, stage.storageTime);		
		assertNotNull(stage.globalMetaDir);
		
		assertNotNull(stage.stageInputDir);
		assertNotNull(stage.stageLogDir);
		assertNotNull(stage.stageTmpDir);
		assertNotNull(stage.stageOutputDir);
				
		assertNotNull(stage.logTable);
		assertNotNull(stage.tfsGlobalMetaDir);
		assertNotNull(stage.tfsTmpDir);
		assertEquals(troilkattProperties, stage.troilkattProperties);
		assertNotNull(stage.logger);
		assertEquals(tfs, stage.tfs);		
	}

	@Test
	public void testConstructor2() throws TroilkattPropertiesException, StageInitException {		
		stage = new Stage(stageNum, stageName, "foo bar baz", localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
				
		assertEquals("003-unitStage", stage.stageName);		
		assertEquals("foo bar baz", stage.args);
		assertNull(stage.tfsOutputDir);		
		assertNull(stage.compressionFormat);		
		assertNotNull(stage.globalMetaDir);
		
		assertNotNull(stage.stageInputDir);
		assertNotNull(stage.stageLogDir);
		assertNotNull(stage.stageTmpDir);
		assertNotNull(stage.stageOutputDir);
		
		assertNotNull(stage.logTable);
		assertNotNull(stage.tfsGlobalMetaDir);
		assertNotNull(stage.tfsTmpDir);
		assertEquals(troilkattProperties, stage.troilkattProperties);
		assertNotNull(stage.logger);
		assertEquals(tfs, stage.tfs);
	}

	// Invalid compression format
	@Test(expected=StageInitException.class)
	public void testConstructor3() throws TroilkattPropertiesException, StageInitException {		
		stage = new Stage(3, "unitStage", "foo bar baz",
				"test/stage", "invalidCompressionFormat", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);		
	}

	// Invalid timestamp
	@Test(expected=StageInitException.class)
	public void testConstructor4() throws TroilkattPropertiesException, StageInitException {		
		stage = new Stage(3, "unitStage", "foo bar baz",
				"test/stage", "gz", -2, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);		
	}
	
	// Invalid HDFS output dir
	// This test will not fail when HDFS is run in pesudo mode
	@Test(expected=StageInitException.class)
	public void testConstructor5() throws TroilkattPropertiesException, StageInitException {		
		stage = new Stage(3, "unitStage", "foo bar baz",
				"/foo/file1", "gz", 3, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);		
	}
	
	// Invalid local root
	@Test(expected=StageInitException.class)
	public void testConstructor6() throws TroilkattPropertiesException, StageInitException {		
		stage = new Stage(3, "unitStage", "foo bar baz",
				"test/stage", "gz", 3, 
				"/foo/bar", hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline);		
	}
	
	// Invalid hdfs meta dir
	// This test will not fail when HDFS is run in pesudo mode
	@Test(expected=StageInitException.class)
	public void testConstructor7() throws TroilkattPropertiesException, StageInitException {		
		stage = new Stage(3, "unitStage", "foo bar baz",
				"test/stage", "gz", 3, 
				localRootDir, "/foo/file1", hdfsStageTmpDir,
				pipeline);		
	}
	
	// Invalid hdfs tmp dir
	// This test will not fail when HDFS is run in pesudo mode
	@Test(expected=StageInitException.class)
	public void testConstructor8() throws TroilkattPropertiesException, StageInitException {		
		stage = new Stage(3, "unitStage", "foo bar baz",
				"test/stage", "gz", 3, 
				localRootDir, hdfsStageMetaDir, "/foo/file1",
				pipeline);		
	}
	
	// Not unit tested
	//@Test
	//public void testProcess2() {
	//}

	// Not unit tested
	//@Test
	//public void testRecover() {
	//}

	@Test
	public void testSetLocalFSDirs() throws StageInitException, TroilkattPropertiesException {
		assertEquals(OsPath.join(tmpDir, "003-unitStage/input"), stage.stageInputDir);
		assertEquals(OsPath.join(tmpDir, "003-unitStage/log"), stage.stageLogDir);
		assertEquals(OsPath.join(tmpDir, "003-unitStage/output"), stage.stageOutputDir);
		assertEquals(OsPath.join(tmpDir, "003-unitStage/meta"), stage.stageMetaDir);
		assertEquals(OsPath.join(tmpDir, "003-unitStage/tmp"), stage.stageTmpDir);
		assertTrue(OsPath.isdir(OsPath.join(tmpDir, "003-unitStage/input")));
		assertTrue(OsPath.isdir(OsPath.join(tmpDir, "003-unitStage/log")));
		assertTrue(OsPath.isdir(OsPath.join(tmpDir, "003-unitStage/output")));
		assertTrue(OsPath.isdir(OsPath.join(tmpDir, "003-unitStage/meta")));
		assertTrue(OsPath.isdir(OsPath.join(tmpDir, "003-unitStage/tmp")));
	}
	
	// Invalid localRootDir
	@Test(expected=StageInitException.class)
	public void testSetLocalFSDirs2() throws StageInitException, TroilkattPropertiesException {
		stage = new Stage(stageNum, stageName, "foo bar baz", "/foo/bar", hdfsStageMetaDir, hdfsStageTmpDir, pipeline);		
	}

	@Test
	public void testSetHDFSDirs() throws IOException, TroilkattPropertiesException, StageInitException {		
		assertTrue(tfs.isdir(OsPath.join(hdfsRoot, "data/test/stage")));				
		assertNotNull(stage.tfsMetaDir);
		assertNotNull(stage.tfsTmpDir);		
		assertEquals(OsPath.join(hdfsRoot, "meta/unitPipeline/003-unitStage"), stage.tfsMetaDir);
		assertEquals(OsPath.join(hdfsRoot, "tmp"), stage.tfsTmpDir);		
		assertTrue(tfs.isdir(stage.tfsMetaDir));
		assertTrue(tfs.isdir(stage.tfsTmpDir));
	}
	
	// Invalid hdfsStageMetaDir
	// This test will not fail when HDFS is run in pesudo mode
	@Test(expected=StageInitException.class)
	public void testSetHDFSDirs2() throws IOException, TroilkattPropertiesException, StageInitException {		
		stage = new Stage(stageNum, stageName, "foo bar baz", localRootDir, "/foo/bar/baz", hdfsStageTmpDir, pipeline);
		assertNotNull(stage);
	}
	
	// Invalid hdfsStageTmpDir
	// This test will not fail when HDFS is run in pesudo mode
	@Test(expected=StageInitException.class)
	public void testSetHDFSDirs3() throws IOException, TroilkattPropertiesException, StageInitException {		
		stage = new Stage(stageNum, stageName, "foo bar baz", localRootDir, hdfsStageMetaDir, "/foo/bar/baz", pipeline);
		assertNotNull(stage);
	}

	@Test
	public void testDownloadInputFiles() throws IOException, StageException {
		ArrayList<String> hdfsFiles = tfs.listdirR(OsPath.join(hdfsRoot, "data/test/input"));
		ArrayList<String> files = stage.downloadInputFiles(hdfsFiles);
		
		assertEquals(6, files.size());
		Collections.sort(files);
		assertTrue(files.get(0).endsWith("file1"));
		assertTrue(files.get(1).endsWith("file2"));
		assertTrue(files.get(2).endsWith("file3"));
		assertTrue(files.get(3).endsWith("file4"));
		assertTrue(files.get(4).endsWith("file5"));
		assertTrue(files.get(5).endsWith("file6"));
		
		for (String f: files) {
			assertTrue(OsPath.isfile(f));
		}
	}
	
	// Invalid file
	@Test(expected=StageException.class)
	public void testDownloadInputFiles2() throws IOException, StageException {
		ArrayList<String> hdfsFiles = tfs.listdirR(OsPath.join(hdfsRoot, "data/test/input"));
		hdfsFiles.add("/non/existing/file");
		ArrayList<String> files = stage.downloadInputFiles(hdfsFiles);
		assertNotNull(files);
	}

	@Test
	public void testDownloadMetaFiles() throws StageException {
		ArrayList<String> files = stage.downloadMetaFiles();
		assertEquals(6, files.size());
		Collections.sort(files);
		assertTrue(files.get(0).endsWith("meta1"));
		assertTrue(files.get(1).endsWith("meta2"));
		assertTrue(files.get(2).endsWith("meta3"));
		assertTrue(files.get(3).endsWith("meta4"));
		assertTrue(files.get(4).endsWith("meta5"));
		assertTrue(files.get(5).endsWith("meta6"));
		
		for (String f: files) {
			assertTrue(OsPath.isfile(f));
		}
	}
	
	// Invalid meta-dir
	// This test will fail with AssertionException (and not StageException) when HDFS run in pesudo mode
	@Test(expected=StageException.class)
	public void testDownloadMetaFiles2() throws StageException {
		stage.tfsMetaDir = "/foo/bar";
		assertNull(stage.downloadMetaFiles());
	}

	@Test(expected=RuntimeException.class)
	public void testProcess() throws StageException {
		ArrayList<String> inputFiles = new ArrayList<String>();
		ArrayList<String> metaFiles = new ArrayList<String>();
		ArrayList<String> logFiles = new ArrayList<String>();
		stage.process(inputFiles, metaFiles, logFiles, 99);
	}

	@Test
	public void testSaveOutputFiles() throws IOException, StageException {
		ArrayList<String> outputFiles = new ArrayList<String>();
		String dstFile1 = OsPath.join(stage.stageOutputDir, "file1");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), dstFile1);
		outputFiles.add(dstFile1);
		String dstFile2 = OsPath.join(stage.stageOutputDir, "file2");
		OsPath.copy(OsPath.join(dataDir, "files/file2"), dstFile2);
		outputFiles.add(dstFile2);
				
		stage.saveOutputFiles(outputFiles, 101);
		assertTrue(tfs.isfile(OsPath.join(hdfsRoot, "data/test/stage/file1.101.bz2")));
		assertTrue(tfs.isfile(OsPath.join(hdfsRoot, "data/test/stage/file2.101.bz2")));
		
		assertTrue(tfs.deleteFile(OsPath.join(hdfsRoot, "data/test/stage/file1.101.bz2")));
		assertTrue(tfs.deleteFile(OsPath.join(hdfsRoot, "data/test/stage/file2.101.bz2")));
	}
	
	// Invalid filename
	@Test(expected=StageException.class)
	public void testSaveOutputFiles2() throws IOException, StageException {
		ArrayList<String> outputFiles = new ArrayList<String>();
		String dstFile1 = OsPath.join(stage.stageOutputDir, "non-existing");	
		outputFiles.add(dstFile1);
		String dstFile2 = OsPath.join(stage.stageOutputDir, "file2");
		OsPath.copy(OsPath.join(dataDir, "files/file2"), dstFile2);
		outputFiles.add(dstFile2);
				
		stage.saveOutputFiles(outputFiles, 102);
	}

	@Test
	public void testSaveMetaFiles() throws StageException, IOException {
		ArrayList<String> files = stage.downloadMetaFiles();
		assertEquals(6, files.size());
		
		String hdfsName = OsPath.join(hdfsRoot, "meta/unitPipeline/003-unitStage/102." + Stage.META_COMPRESSION);
		assertFalse(tfs.isfile(hdfsName));
		stage.saveMetaFiles(files, 102);
		assertTrue(tfs.isfile(hdfsName));
		tfs.deleteDir(hdfsName);
	}
	
	@Test(expected=StageException.class)
	public void testSaveMetaFiles2() throws StageException, IOException {
		ArrayList<String> files = new ArrayList<String>();
		files.add("/foo/bar/ab");		
		stage.saveMetaFiles(files, 103);
	}
	
	// THis test is problematic due to the clearTable call
	@Test
	public void testSaveLogFiles() throws IOException, StageException, PipelineException, HbaseException {
		ArrayList<String> logFiles = new ArrayList<String>();
		String dstFile1 = OsPath.join(stage.stageOutputDir, "1.log");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), dstFile1);
		logFiles.add(dstFile1);
		String dstFile2 = OsPath.join(stage.stageOutputDir, "2.out");
		OsPath.copy(OsPath.join(dataDir, "files/file2"), dstFile2);
		logFiles.add(dstFile2);
		String dstFile3 = OsPath.join(stage.stageOutputDir, "3.unknown");
		OsPath.copy(OsPath.join(dataDir, "files/file2"), dstFile3);
		logFiles.add(dstFile3);
		String dstFile4 = OsPath.join(stage.stageOutputDir, "4.log");
		OsPath.copy(OsPath.join(dataDir, "files/file2"), dstFile4);
		logFiles.add(dstFile4);
				
		stage.saveLogFiles(logFiles, 101);
		assertTrue(stage.logTable.containsFile("003-unitStage", 101, "1.log"));
		assertTrue(stage.logTable.containsFile("003-unitStage", 101, "2.out"));
		assertTrue(stage.logTable.containsFile("003-unitStage", 101, "3.unknown"));
		assertTrue(stage.logTable.containsFile("003-unitStage", 101, "4.log"));
		assertFalse(stage.logTable.containsFile("003-unitStage", 101, "5.out"));
		stage.logTable.schema.clearTable();
	}
	
	@Test
	public void testSaveLogFiles2() throws StageException, IOException {
		ArrayList<String> files = new ArrayList<String>();
		files.add("/foo/bar/ab");		
		assertEquals(0, stage.saveLogFiles(files, 103));
	}

	// Not unit tested
	//@Test
	//public void testProcess2() {
	//}
	
	// Not unit tested
	//@Test
	//public void testRecover() {
	//}
	
	@Test
	public void testCleanup() throws StageInitException, StageException, IOException, TroilkattPropertiesException {
		stage = new Stage(stageNum,stageName, "foo bar baz", localRootDir, hdfsStageMetaDir, hdfsStageTmpDir, pipeline);
		
		assertTrue(OsPath.isdir(OsPath.join(tmpDir, "003-unitStage/input")));
		assertTrue(OsPath.isdir(OsPath.join(tmpDir, "003-unitStage/log")));
		assertTrue(OsPath.isdir(OsPath.join(tmpDir, "003-unitStage/output")));
		assertTrue(OsPath.isdir(OsPath.join(tmpDir, "003-unitStage/meta")));
		assertTrue(OsPath.isdir(OsPath.join(tmpDir, "003-unitStage/tmp")));
		assertTrue(tfs.isdir(stage.tfsTmpDir));
		
		stage.cleanupLocalDirs();
		assertEquals(0, OsPath.listdir(OsPath.join(tmpDir, "003-unitStage/input"), testLogger).length);
		assertEquals(0, OsPath.listdir(OsPath.join(tmpDir, "003-unitStage/log"), testLogger).length);
		assertEquals(0, OsPath.listdir(OsPath.join(tmpDir, "003-unitStage/output"), testLogger).length);
		assertEquals(0, OsPath.listdir(OsPath.join(tmpDir, "003-unitStage/meta"), testLogger).length);
		assertEquals(0, OsPath.listdir(OsPath.join(tmpDir, "003-unitStage/tmp"), testLogger).length);
		
		assertTrue(tfs.isdir(stage.tfsTmpDir));
		stage.cleanupTFSDirs();
		assertNull(tfs.listdir(stage.tfsTmpDir));
	}

	@Test
	public void testGetStageID() {
		assertEquals("unitPipeline-003-unitStage", stage.getStageID());
	}

	@Test
	public void testIsSource() {
		assertFalse(stage.isSource());
	}

	@Test
	public void testGetOutputFiles() throws IOException, StageException {
		String dstFile1 = OsPath.join(stage.stageOutputDir, "file1");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), dstFile1);
		String dstFile2 = OsPath.join(stage.stageOutputDir, "file2");
		OsPath.copy(OsPath.join(dataDir, "files/file2"), dstFile2);		
		
		ArrayList<String> outputFiles = stage.getOutputFiles();
		assertEquals(outputFiles.size(), 2);
		Collections.sort(outputFiles);
		assertTrue(outputFiles.get(0).endsWith("file1"));
		assertTrue(outputFiles.get(1).endsWith("file2"));
	}

	@Test
	public void testUpdateMetaFiles() throws StageException, IOException {
		ArrayList<String> files = stage.downloadMetaFiles();
		assertEquals(6, files.size());
		
		OsPath.copy(OsPath.join(dataDir, "files/file1"), 
				OsPath.join(stage.stageMetaDir, "meta7"));
		
		stage.updateMetaFiles(files);
		assertEquals(7, files.size());
		Collections.sort(files);
		assertTrue(files.get(6).endsWith("meta7"));
	}

	@Test
	public void testUpdateLogFiles() throws IOException, StageException {
		String dstFile1 = OsPath.join(stage.stageLogDir, "log1");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), dstFile1);
		
		ArrayList<String> logFiles = new ArrayList<String>();
		stage.updateLogFiles(logFiles);
		
		assertEquals(logFiles.size(), 1);
		assertTrue(logFiles.get(0).endsWith("log1"));
		
		String dstFile2 = OsPath.join(stage.stageLogDir, "log2");
		OsPath.copy(OsPath.join(dataDir, "files/file2"), dstFile2);
		
		stage.updateLogFiles(logFiles);
		assertEquals(logFiles.size(), 2);
		Collections.sort(logFiles);
		assertTrue(logFiles.get(0).endsWith("log1"));
		assertTrue(logFiles.get(1).endsWith("log2"));
	}

	@Test
	public void testSetTroilkattSymbols() throws TroilkattPropertiesException {
		assertEquals(OsPath.normPath(stage.stageInputDir), stage.setTroilkattSymbols("TROILKATT.INPUT_DIR"));
		assertEquals(OsPath.normPath(stage.stageOutputDir), stage.setTroilkattSymbols("TROILKATT.OUTPUT_DIR"));
		assertEquals(OsPath.normPath(stage.stageLogDir), stage.setTroilkattSymbols("TROILKATT.LOG_DIR")); 
		assertEquals(OsPath.normPath(stage.stageMetaDir), stage.setTroilkattSymbols("TROILKATT.META_DIR"));
		assertEquals(OsPath.normPath(stage.stageTmpDir), stage.setTroilkattSymbols("TROILKATT.TMP_DIR")); 
		assertEquals(OsPath.normPath(troilkattProperties.get("troilkatt.localfs.dir")), stage.setTroilkattSymbols("TROILKATT.DIR"));
		assertEquals(OsPath.normPath(troilkattProperties.get("troilkatt.localfs.binary.dir")), stage.setTroilkattSymbols("TROILKATT.BIN")); 
		assertEquals(OsPath.normPath(troilkattProperties.get("troilkatt.localfs.utils.dir")), stage.setTroilkattSymbols("TROILKATT.UTILS")); 
		assertEquals(OsPath.normPath(troilkattProperties.get("troilkatt.globalfs.global-meta.dir")), stage.setTroilkattSymbols("TROILKATT.GLOBALMETA_DIR"));
		assertEquals(OsPath.normPath(troilkattProperties.get("troilkatt.localfs.scripts.dir")), stage.setTroilkattSymbols("TROILKATT.SCRIPTS")); 
		assertEquals(">", stage.setTroilkattSymbols("TROILKATT.REDIRECT_OUTPUT")); 
		assertEquals("2>", stage.setTroilkattSymbols("TROILKATT.REDIRECT_ERROR"));
		assertEquals("<", stage.setTroilkattSymbols("TROILKATT.REDIRECT_INPUT"));
		assertEquals(";", stage.setTroilkattSymbols("TROILKATT.SEPERATE_COMMAND"));
		
		assertEquals(OsPath.normPath(stage.stageInputDir) + " " + OsPath.normPath(stage.stageOutputDir), 
				stage.setTroilkattSymbols("TROILKATT.INPUT_DIR TROILKATT.OUTPUT_DIR"));
	}

	@Test
	public void testSetTroilkattFilenameSymbols() {
		assertEquals("bar", stage.setTroilkattFilenameSymbols("TROILKATT.FILE_NOEXT", "foo/bar.baz.bongo")); 
		assertEquals("bar.baz.bongo", stage.setTroilkattFilenameSymbols("TROILKATT.FILE", "foo/bar.baz.bongo"));
	}

	@Test
	public void testExecuteCmdStringLogger() {
		/* The test program is a simple program that takes N arguments. The first
		 * is the total number of arguments, the second is the programs return value,
		 * and the following can be anything (they are ignored) */
		String testCmd = OsPath.join(dataDir, "bin/unit-test-program");
		assertEquals(0, Stage.executeCmd(testCmd + " 2 0", testLogger));
		assertEquals(1, Stage.executeCmd(testCmd + " 2 1", testLogger));		
		assertEquals(0, Stage.executeCmd(testCmd + " 5 0 foo bar baz", testLogger));
		assertEquals(127, Stage.executeCmd("non-existing-program", testLogger));
		assertEquals(254, Stage.executeCmd(testCmd, testLogger)); // -2
	}
	
	@Test
	public void testSplitArgs() throws StageInitException {
		String[] split = stage.splitArgs("foo bar baz");
		assertEquals(3, split.length);
		assertEquals("foo", split[0]);
		assertEquals("bar", split[1]);
		assertEquals("baz", split[2]);
		
		split = stage.splitArgs("foo 'bar baz'");
		assertEquals(2, split.length);
		assertEquals("foo", split[0]);
		assertEquals("bar baz", split[1]);
		
		split = stage.splitArgs("'foo bar baz'");
		assertEquals(1, split.length);
		assertEquals("foo bar baz", split[0]);
		
		split = stage.splitArgs("foo\t'bar baz'");
		assertEquals(2, split.length);
		assertEquals("foo", split[0]);
		assertEquals("bar baz", split[1]);
		
		split = stage.splitArgs("foo 'bar\tbaz'");
		assertEquals(2, split.length);
		assertEquals("foo", split[0]);
		assertEquals("bar\tbaz", split[1]);	
		
		split = stage.splitArgs("");
		assertEquals(0, split.length);		
	}
	
	@Test(expected=StageInitException.class)
	public void testSplitArgs2() throws StageInitException {
		String[] split = stage.splitArgs("foo 'bar baz");
		assertEquals(3, split.length);
	}
}
