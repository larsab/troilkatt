package edu.princeton.function.troilkatt.pipeline;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

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
import edu.princeton.function.troilkatt.mapreduce.TroilkattMapReduce;

public class SGEStageTest extends TestSuper {
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

		tfs = new TroilkattNFS();
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs);

		nfsOutput = OsPath.join(outDir, "sgeOutput");

		stageNum = 7;
		stageName = "sgestage";
		localRootDir = tmpDir;
		String nfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), OsPath.join("meta", pipeline.name));
		nfsStageMetaDir = OsPath.join(nfsPipelineMetaDir, String.format("%03d-%s", stageNum, stageName));
		nfsTmpDir = troilkattProperties.get("troilkatt.tfs.root.dir");
		nfsTmpLogDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "log");
		nfsTmpOutputDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "output");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		sges = new SGEStage(stageNum, stageName, " execute_per_file " + executeCmd,
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

	@Test
	public void testSGEStage() throws TroilkattPropertiesException {		
		assertTrue(sges.sgeCmd.startsWith("submit -s"));
		assertTrue(sges.scriptFilename.endsWith("sge.sh"));
		assertTrue(sges.argsFilename.endsWith("sge.args"));
		assertTrue(sges.inputFilesFilename.startsWith("sge.files"));
		assertEquals(troilkattProperties.get("troilkatt.globalfs.sge.dir"), sges.sgeDir);
		assertEquals(troilkattProperties.get("troilkatt.jar"), sges.jarFile);
		assertEquals(sges.mainClass, "edu.princeton.function.troilkatt.sge.ExecuteStage");
	}

	@Test
	public void testWriteSGEArgsFile() throws StageException, StageInitException, IOException, TroilkattPropertiesException {
		sges.writeSGEArgsFile("/nfs/tmp/output", "/nfs/tmp/log", 3216);
		
		BufferedReader ib = new BufferedReader(new FileReader(sges.argsFilename));
		
		// Note! read order of lines must match write order in MapReduce.writeMapReduceArgsFile
		assertEquals(troilkattProperties.getConfigFile(), TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "configuration.file"));
		assertEquals(sges.pipelineName, TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "pipeline.name"));		
		assertEquals("008-mapreduce-unittest", TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "stage.name"));		
		assertEquals("atn1 vcsd1", TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "stage.args"));		
		assertEquals("execute_per_file " + executeCmd, TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "hdfs.output.dir"));		
		assertEquals("/nfs/tmp/output", TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "nfs.output.dir"));		
		assertEquals("gz", TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "compression.format"));
		assertTrue(Long.valueOf(TroilkattMapReduce.checkKeyGetValLong(ib.readLine(), "storage.time")) == -1);			
		assertEquals("/nfs/tmp/log", TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "nfs.log.dir"));		
		assertEquals(sges.hdfsMetaDir, TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "hdfs.meta.dir"));	
		assertEquals(OsPath.join(troilkattProperties.get("troilkatt.localfs.sge.dir"), "pipeline"), TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "sge.pipeline.dir"));
		assertEquals(OsPath.join(troilkattProperties.get("troilkatt.localfs.sge.dir"), "tmp"), TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "sge.tmp.dir"));
		assertNotNull(TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "logging.level"));		
		assertTrue(Long.valueOf(TroilkattMapReduce.checkKeyGetValLong(ib.readLine(), "timestamp")) == 567);
						
		ib.close();		
	}

	@Test
	public void testWriteSGEScript() throws StageException, IOException {
		sges.writeSGEScript(inputFiles);
		
		/*
		 * .sh
		 */
		BufferedReader ib = new BufferedReader(new FileReader(sges.scriptFilename));		
		assertEquals("#!/bin/sh", ib.readLine());						
		ib.close();	
		
		/*
		 * .files
		 */
		ib = new BufferedReader(new FileReader(sges.inputFilesFilename));
		ArrayList<String> inputFiles2 = new ArrayList<String>();
		while (true) {
			String str = ib.readLine();
			if (str == null) {
				break;
			}			
			inputFiles2.add(str.trim());
		}
		ib.close();
		
		assertEquals(inputFiles.size(), inputFiles2.size());
		for (String f: inputFiles) {
			assertTrue(inputFiles2.contains(f));
		}
	}

	@Test
	public void testMoveSGELogFiles() throws StageException {
		ArrayList<String> logFiles = new ArrayList<String>();
		// 3 in "tmp" log
		String tmpLogDir = OsPath.join(tmpDir, "move");
		String dstFile1 = OsPath.join(tmpLogDir, "1.log");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), dstFile1);
		String dstFile2 = OsPath.join(tmpLogDir, "2.out");
		OsPath.copy(OsPath.join(dataDir, "files/file2"), dstFile2);
		String dstFile3 = OsPath.join(tmpLogDir, "3.unknown");
		OsPath.copy(OsPath.join(dataDir, "files/file2"), dstFile3);
		
		// 1 in log
		String finalLogDir = OsPath.join(tmpDir, "dst");
		String dstFile4 = OsPath.join(finalLogDir, "4.log");
		OsPath.copy(OsPath.join(dataDir, "files/file2"), dstFile4);
		logFiles.add(dstFile4);	
		
		assertEquals(3, OsPath.listdir(tmpLogDir).length);
		sges.moveSGELogFiles(tmpLogDir, logFiles);
		assertEquals(0, OsPath.listdir(tmpLogDir).length);
		assertEquals(4, logFiles.size());
		assertTrue(OsPath.isfile(OsPath.join(finalLogDir, "1.log")));
		assertTrue(OsPath.isfile(OsPath.join(finalLogDir, "2.log")));
		assertTrue(logFiles.contains(OsPath.join(finalLogDir, "1.log")));
		assertTrue(OsPath.isfile(OsPath.join(finalLogDir, "3.log")));
		assertTrue(OsPath.isfile(OsPath.join(finalLogDir, "4.log")));
	}

	//@Test
	//public void testSaveOutputFiles() {
	//	fail("Not yet implemented");
	//}

	@Test
	public void testProcess() throws IOException, StageException {
		ArrayList<String> metaFiles = writeMetaFile(inputBasenames);
		ArrayList<String> logFiles = new ArrayList<String>();
		ArrayList<String> outputFiles = sges.process(inputFiles, metaFiles, logFiles, 741);
		
		/*
		 * Test output files
		 */
		assertEquals(4, outputFiles.size());
		Collections.sort(outputFiles);
		assertTrue(outputFiles.get(0).endsWith("file1.out.741.gz"));
		assertTrue(outputFiles.get(1).endsWith("file2.out.741.gz"));
		assertTrue(outputFiles.get(3).endsWith("file4.out.741.gz"));
		assertTrue(tfs.isfile(OsPath.join(sges.hdfsOutputDir, "file1.out.741.gz")));
		assertTrue(tfs.isfile(OsPath.join(sges.hdfsOutputDir, "file2.out.741.gz")));
		assertTrue(tfs.isfile(OsPath.join(sges.hdfsOutputDir, "file4.out.741.gz")));
		
		/*
		 * Test MapReduce logfiles
		 */		
		assertFalse(logFiles.isEmpty());
		assertEquals(8, logFiles.size());
		//assertTrue(OsPath.fileInList(mapperFiles, "stdout", false));
		//assertTrue(OsPath.fileInList(mapperFiles, "stderr", false));
		//assertTrue(OsPath.fileInList(mapperFiles, "syslog", false));		
	}

	private ArrayList<String> writeMetaFile(ArrayList<String> inputFiles) throws IOException, StageException {
		String metaFilename = OsPath.join(sges.stageMetaDir, "filelist");
		FSUtils.writeTextFile(metaFilename, inputFiles);
		
		String[] succesLine = {"success\n"};
		String mapFilename = OsPath.join(sges.stageMetaDir, "maptest");
		FSUtils.writeTextFile(mapFilename, succesLine);
		
		String reduceFilename = OsPath.join(sges.stageMetaDir, "reducetest");
		FSUtils.writeTextFile(reduceFilename, succesLine);
		
		ArrayList<String> metaFiles = new ArrayList<String>();
		metaFiles.add(metaFilename);
		metaFiles.add(mapFilename);
		metaFiles.add(reduceFilename);
		
		return metaFiles;
	}
}
