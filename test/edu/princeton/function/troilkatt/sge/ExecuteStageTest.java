package edu.princeton.function.troilkatt.sge;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass; 
import org.junit.Test;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TestSuper;
import edu.princeton.function.troilkatt.Troilkatt;
import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattNFS;
import edu.princeton.function.troilkatt.pipeline.ExecutePerFile;
import edu.princeton.function.troilkatt.pipeline.SGEStage;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class ExecuteStageTest extends TestSuper {
	protected static TroilkattNFS tfs;
	protected static Pipeline pipeline;	
	protected static TroilkattProperties troilkattProperties;
	protected static String nfsOutput;
	protected static Logger testLogger;
	public static String executeCmd = "/usr/bin/python " + OsPath.join(dataDir, "bin/executePerFileTest.py") + " TROILKATT.INPUT_DIR/TROILKATT.FILE TROILKATT.OUTPUT_DIR/TROILKATT.FILE_NOEXT.out TROILKATT.META_DIR/filelist > TROILKATT.LOG_DIR/executePerFileTest.out 2> TROILKATT.LOG_DIR/executePerFileTest.log";

	protected SGEStage sges;
	protected ArrayList<String> inputFiles;
	protected ArrayList<String> inputBasenames;

	protected static int stageNum;
	protected static String stageName;
	protected static String localRootDir;
	protected static String nfsStageMetaDir;	
	
	protected static String nfsTmpOutputDir;
	protected static String nfsTmpLogDir;
	protected static String nfsTmpDir;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		testLogger = Logger.getLogger("test");
		TestSuper.initTestDir();
		
		troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFileNFS));	
		
		nfsTmpDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "tmp");
		nfsTmpLogDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "log");
		nfsTmpOutputDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "output");
		OsPath.mkdir(nfsTmpDir);
		OsPath.mkdir(nfsTmpLogDir);
		OsPath.mkdir(nfsTmpOutputDir);
		
		OsPath.mkdir(troilkattProperties.get("troilkatt.localfs.sge.dir"));		
		String nfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "unitPipeline/meta");
		nfsStageMetaDir = OsPath.join(nfsPipelineMetaDir, String.format("%03d-%s", stageNum, stageName));
		OsPath.mkdir(nfsStageMetaDir);

		tfs = new TroilkattNFS();
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs);

		nfsOutput = OsPath.join(outDir, "sgeOutput");
		OsPath.mkdir(nfsOutput);

		stageNum = 7;
		stageName = "sgestage";
		localRootDir = tmpDir;		
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		sges = new SGEStage(stageNum, stageName, "execute_per_file " + executeCmd,
				nfsOutput, "gz", -1, 
				localRootDir, nfsStageMetaDir, nfsTmpDir,
				pipeline);

		String inputDir = "troilkatt/data/test/mapreduce/input";
		inputFiles = new ArrayList<String>();
		inputFiles.add(OsPath.join(inputDir, "file1.1.gz"));
		inputFiles.add(OsPath.join(inputDir, "file2.1.gz"));
		inputFiles.add(OsPath.join(inputDir, "file3.1.gz"));
		inputFiles.add(OsPath.join(inputDir, "file4.1.none"));

		inputBasenames = new ArrayList<String>();
		for (String f: inputFiles) {
			String b = OsPath.basename(f);
			inputBasenames.add(b.split("\\.")[0]);
		}
	}

	@After
	public void tearDown() throws Exception {
	}

	// Tested by testExecuteStage
	//@Test
	//public void testReadSGEArgsFile() throws StageException {
	//	
	//}

	@Test
	public void testExecuteStage() throws StageException, StageInitException, TroilkattPropertiesException {
		sges.writeSGEArgsFile(nfsTmpOutputDir, nfsTmpLogDir, 3217);
		
		ExecuteStage es = new ExecuteStage(sges.argsFilename, "task1");
		
		assertEquals(OsPath.join(dataDir, configurationFileNFS), es.configurationFile);
		assertEquals(pipeline.name, es.pipelineName);				
		assertEquals(sges.stageName, es.stageName);
		assertEquals(sges.args, es.args);
		assertEquals("task1", es.taskID);
		assertEquals("sgestage-task1", es.taskStageName);	
		assertEquals(nfsTmpOutputDir, es.nfsOutputDir);		
		assertEquals(sges.compressionFormat, es.compressionFormat);
		assertEquals(sges.storageTime, es.storageTime);			
		assertEquals(sges.hdfsMetaDir, es.nfsMetaDir);	
		assertEquals(nfsTmpLogDir, es.nfsLogDir);
		String localSgeDir = troilkattProperties.get("troilkatt.localfs.sge.dir");
		assertEquals(OsPath.join(localSgeDir, "pipeline"), es.localFSPipelineDir);
		assertEquals(OsPath.join(localSgeDir, "tmp"), es.localFSTmpDir);			
		assertEquals("info", es.loggingLevel);
		assertEquals(3217, es.timestamp);				
		assertNotNull(es.logger);
		assertNotNull(es.troilkattProperties);
		assertNotNull(es.tfs);
		
		/*
		 *  Stage initialization tests
		 */
		assertNotNull(es.stage);
		assertTrue(es.stage instanceof ExecutePerFile);
		assertTrue(es.stage.args.startsWith("/usr/bin/python /home/larsab/troilkatt2/test-data/bin/executePerFileTest.py"));
		assertEquals(OsPath.join(es.localFSPipelineDir, "007-sgestage-task1/input"), es.stage.stageInputDir);
		assertEquals(nfsTmpOutputDir,  es.stage.hdfsOutputDir);
	}

	@Test
	public void testRun() throws StageException, IOException, StageInitException {		
		ExecuteStage es = new ExecuteStage(sges.argsFilename, "task1");
		
		// Push meta-file to log-tar
		String metaFilename = OsPath.join(sges.stageMetaDir, "filelist");
		FSUtils.writeTextFile(metaFilename, inputFiles);
		ArrayList<String> metaFiles = new ArrayList<String>();
		sges.saveMetaFiles(metaFiles, 3218);
		OsPath.delete(metaFilename);
		
		sges.writeSGEArgsFile(nfsTmpOutputDir, nfsTmpLogDir, 3219);
		
		// Run ExecuteStage for one file
		es.run(inputFiles.get(0));
		
		// Check output files
		String[] outputFiles = OsPath.listdir(nfsTmpOutputDir);
		assertEquals(1, outputFiles.length);
		assertTrue(outputFiles[0].equals("file1.out.3219.gz"));
		assertTrue(tfs.isfile(OsPath.join(nfsTmpOutputDir, "file1.out.3219.gz")));
		
		// Check log files
		String[] logFiles = OsPath.listdir(nfsTmpLogDir);
		assertEquals(1, logFiles.length);
		Arrays.sort(logFiles);
		assertTrue(logFiles[0].equals("executePerFileTest.log"));
	}
}
