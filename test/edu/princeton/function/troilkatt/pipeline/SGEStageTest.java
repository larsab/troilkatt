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
import edu.princeton.function.troilkatt.TestSuperNFS;
import edu.princeton.function.troilkatt.Troilkatt;
import edu.princeton.function.troilkatt.TroilkattProperties;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattNFS;
import edu.princeton.function.troilkatt.mapreduce.TroilkattMapReduce;

public class SGEStageTest extends TestSuperNFS {
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
		TestSuperNFS.initNFSTestDir();

		troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFileNFS));	

		tfs = new TroilkattNFS();
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs);

		nfsOutput = OsPath.join(outDir, "sgeOutput");

		stageNum = 7;
		stageName = "sgestage";
		localRootDir = tmpDir;
		String nfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), OsPath.join("meta", pipeline.name));
		nfsStageMetaDir = OsPath.join(nfsPipelineMetaDir, String.format("%03d-%s", stageNum, stageName));
		nfsTmpDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "tmp");
		nfsTmpLogDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "log");
		nfsTmpOutputDir = OsPath.join(troilkattProperties.get("troilkatt.tfs.root.dir"), "output");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		sges = new SGEStage(stageNum, stageName, "execute_per_file 1 512 " + executeCmd,
				nfsOutput, "gz", -1, 
				localRootDir, nfsStageMetaDir, nfsTmpDir,
				pipeline);

		String inputDir = OsPath.join(nfsRoot, "test/sge/input");	
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
		assertEquals("sge.sh", OsPath.basename(sges.scriptFilename));
		assertEquals("sge.args", OsPath.basename(sges.argsFilename));		
		assertEquals(troilkattProperties.get("troilkatt.globalfs.sge.dir"), sges.sgeDir);		
		assertEquals(1, sges.maxProcs);
		assertEquals(512, sges.maxVMSize);
	}

	@Test
	public void testWriteSGEArgsFile() throws StageException, StageInitException, IOException, TroilkattPropertiesException {
		sges.writeSGEArgsFile("/nfs/tmp/output", "/nfs/tmp/log", 3216, inputFiles);
		
		BufferedReader ib = new BufferedReader(new FileReader(sges.argsFilename));
		
		// Note! read order of lines must match write order in MapReduce.writeMapReduceArgsFile
		assertEquals(troilkattProperties.getConfigFile(), TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "configuration.file"));
		assertEquals(sges.pipelineName, TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "pipeline.name"));		
		assertEquals("007-sgestage", TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "stage.name"));		
		assertEquals("execute_per_file " + executeCmd, TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "stage.args"));					
		assertEquals("/nfs/tmp/output", TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "nfs.output.dir"));		
		assertEquals("gz", TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "compression.format"));
		assertTrue(Long.valueOf(TroilkattMapReduce.checkKeyGetValLong(ib.readLine(), "storage.time")) == -1);			
		assertEquals("/nfs/tmp/log", TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "nfs.log.dir"));		
		assertEquals(sges.hdfsMetaDir, TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "nfs.meta.dir"));	
		assertEquals(OsPath.join(troilkattProperties.get("troilkatt.localfs.sge.dir"), "pipeline"), TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "sge.pipeline.dir"));
		assertEquals(OsPath.join(troilkattProperties.get("troilkatt.localfs.sge.dir"), "tmp"), TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "sge.tmp.dir"));
		assertNotNull(TroilkattMapReduce.checkKeyGetVal(ib.readLine(), "logging.level"));		
		assertTrue(Long.valueOf(TroilkattMapReduce.checkKeyGetValLong(ib.readLine(), "timestamp")) == 3216);
		assertTrue(Long.valueOf(TroilkattMapReduce.checkKeyGetValLong(ib.readLine(), "max.num.procs")) == 1);
		assertTrue(Long.valueOf(TroilkattMapReduce.checkKeyGetValLong(ib.readLine(), "max.vm.size")) == 512);
		assertEquals("input.files.start", ib.readLine());
		
		ArrayList<String> inputFiles2 = new ArrayList<String>();
		while (true) {
			String str = ib.readLine();
			if (str == null) {
				fail("input.files.end was not found");
			}
			if (str.equals("input.files.end")) {
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
	public void testWriteSGEScript() throws StageException, IOException {
		sges.writeSGEScript("/nfs/tmp/log");
		
		/*
		 * .sh
		 */
		BufferedReader ib = new BufferedReader(new FileReader(sges.scriptFilename));		
		assertEquals("#!/bin/sh", ib.readLine());						
		ib.close();	
	}

	@Test
	public void testMoveSGELogFiles() throws StageException {
		ArrayList<String> logFiles = new ArrayList<String>();
		// 3 in "tmp" log
		String tmpLogDir = OsPath.join(tmpDir, "move");
		OsPath.deleteAll(tmpLogDir);
		OsPath.mkdir(tmpLogDir);
		String dstFile1 = OsPath.join(tmpLogDir, "1.log");
		OsPath.copy(OsPath.join(dataDir, "files/file1"), dstFile1);
		String dstFile2 = OsPath.join(tmpLogDir, "2.log");
		OsPath.copy(OsPath.join(dataDir, "files/file2"), dstFile2);
		String dstFile3 = OsPath.join(tmpLogDir, "3.log");
		OsPath.copy(OsPath.join(dataDir, "files/file2"), dstFile3);
		
		// 1 in log
		String finalLogDir = sges.stageLogDir;
		OsPath.deleteAll(finalLogDir);
		OsPath.mkdir(finalLogDir);
		
		// Put one fiel into final log directory
		String dstFile4 = OsPath.join(finalLogDir, "4.log");
		OsPath.copy(OsPath.join(dataDir, "files/file2"), dstFile4);
		logFiles.add(dstFile4);	
		
		assertEquals(3, OsPath.listdir(tmpLogDir).length);
		sges.moveSGELogFiles(tmpLogDir, logFiles);
		assertEquals(0, OsPath.listdir(tmpLogDir).length);
		assertEquals(4, logFiles.size());
		assertTrue(logFiles.contains(OsPath.join(finalLogDir, "1.log")));
		assertTrue(logFiles.contains(OsPath.join(finalLogDir, "2.log")));
		
		assertTrue(OsPath.isfile(OsPath.join(finalLogDir, "1.log")));
		assertTrue(OsPath.isfile(OsPath.join(finalLogDir, "2.log")));		
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
		sges.saveMetaFiles(metaFiles, 3218);		
		
		ArrayList<String> logFiles = new ArrayList<String>();
		
		ArrayList<String> outputFiles = sges.process(inputFiles, metaFiles, logFiles, 3219);
		
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
		
		ArrayList<String> metaFiles = new ArrayList<String>();
		metaFiles.add(metaFilename);
		
		return metaFiles;
	}
}
