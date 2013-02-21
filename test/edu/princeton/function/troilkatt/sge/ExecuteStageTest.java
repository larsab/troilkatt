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
import edu.princeton.function.troilkatt.TestSuperNFS;
import edu.princeton.function.troilkatt.Troilkatt;
import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattNFS;
import edu.princeton.function.troilkatt.pipeline.ExecutePerFileSGE;
import edu.princeton.function.troilkatt.pipeline.SGEStage;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class ExecuteStageTest extends TestSuperNFS {
	protected static TroilkattNFS tfs;
	protected static Pipeline pipeline;	
	protected static TroilkattProperties troilkattProperties;
	protected static String nfsOutput;
	protected static Logger testLogger;
	public static String executeCmd = "1 256 /usr/bin/python " + OsPath.join(dataDir, "bin/executePerFileTest.py") + " TROILKATT.INPUT_DIR/TROILKATT.FILE TROILKATT.OUTPUT_DIR/TROILKATT.FILE_NOEXT.out TROILKATT.META_DIR/filelist > TROILKATT.LOG_DIR/executePerFileTest.out 2> TROILKATT.LOG_DIR/executePerFileTest.log";

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
		TestSuperNFS.initNFSTestDir();
		
		troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFileNFS));	
		
		nfsTmpDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "tmp");
		nfsTmpLogDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "log");
		nfsTmpOutputDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "output");
		OsPath.deleteAll(nfsTmpDir);
		OsPath.deleteAll(nfsTmpLogDir);
		OsPath.deleteAll(nfsTmpOutputDir);
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
		
		inputFiles = new ArrayList<String>();
		inputFiles.add(OsPath.join(dataDir, "files/file1.1.gz"));
		inputFiles.add(OsPath.join(dataDir, "files/file2.1.gz"));
		inputFiles.add(OsPath.join(dataDir, "files/file3.1.gz"));
		inputFiles.add(OsPath.join(dataDir, "files/file4.1.none"));

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
		sges.writeSGEArgsFile(nfsTmpOutputDir, nfsTmpLogDir, 3217, inputFiles);
		
		ExecuteStage es = new ExecuteStage(sges.argsFilename, 0, "job1");
		
		assertEquals(OsPath.join(dataDir, configurationFileNFS), es.configurationFile);
		assertEquals(pipeline.name, es.pipelineName);				
		assertEquals(sges.stageName, es.stageName);
		//assertEquals(sges.args, es.args);
		assertEquals("task_0", es.taskID);
		assertEquals("sgestage-task_0", es.taskStageName);	
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
		assertEquals(1, es.maxProcs);
		assertEquals(256, es.maxVMSize);
		
		/*
		 *  Stage initialization tests
		 */
		assertNotNull(es.stage);
		assertTrue(es.stage instanceof ExecutePerFileSGE);
		assertTrue(es.stage.args.startsWith("/usr/bin/python /home/larsab/troilkatt2/test-data/bin/executePerFileTest.py"));
		assertEquals(OsPath.join(es.localFSPipelineDir, "007-sgestage-task_0/input"), es.stage.stageInputDir);
		assertEquals(nfsTmpOutputDir,  es.stage.hdfsOutputDir);
	}

	@Test
	public void testRun() throws StageException, IOException, StageInitException {		
		sges.writeSGEArgsFile(nfsTmpOutputDir, nfsTmpLogDir, 3219, inputFiles);
		
		ExecuteStage es = new ExecuteStage(sges.argsFilename, 0, "task1");
		
		// Push meta-file to log-tar
		String metaFilename = OsPath.join(sges.stageMetaDir, "filelist");
		ArrayList<String> basenames = new ArrayList<String>();
		basenames.add(tfs.getFilenameName(inputFiles.get(0)));
		FSUtils.writeTextFile(metaFilename, basenames);
		ArrayList<String> metaFiles = new ArrayList<String>();
		metaFiles.add(metaFilename);
		sges.saveMetaFiles(metaFiles, 3218);
		OsPath.delete(metaFilename);
				
		// Run ExecuteStage for one file
		es.process2(inputFiles.get(0));
		
		// Check output files
		String[] outputFiles = OsPath.listdir(nfsTmpOutputDir);
		assertEquals(1, outputFiles.length);
		assertTrue(OsPath.basename(outputFiles[0]).equals("file1.out.3219.gz"));
		assertTrue(tfs.isfile(OsPath.join(nfsTmpOutputDir, "file1.out.3219.gz")));
		
		// Check log files
		String[] logFiles = OsPath.listdirR(nfsTmpLogDir);
		assertEquals(2, logFiles.length);
		Arrays.sort(logFiles);
		assertTrue(logFiles[0].endsWith("executePerFileTest.log"));
		assertTrue(logFiles[1].endsWith("executePerFileTest.out"));
	}
}
