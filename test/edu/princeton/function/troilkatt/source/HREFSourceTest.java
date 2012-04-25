package edu.princeton.function.troilkatt.source;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import edu.princeton.function.troilkatt.fs.TroilkattFS;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

public class HREFSourceTest extends TestSuper {
	protected static TroilkattProperties troilkattProperties;				
	protected static TroilkattFS tfs;	
	protected static Pipeline pipeline;
	
	protected static String localRootDir;
	protected static String hdfsStageMetaDir;
	protected static String hdfsStageTmpDir;
	
	public static final String pageURL = "http://brainarray.mbni.med.umich.edu/Brainarray/Database/CustomCDF/14.1.0/entrezg.asp"; 
	public static final String filter = ".*Dm_ENTREZG_.*\\.zip";
	private static String localVersion;
	private static String args;
	
	protected HREFSource source;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		FileSystem hdfs = FileSystem.get(new Configuration());
		tfs = new TroilkattFS(hdfs);		
		troilkattProperties = Troilkatt.getProperties(OsPath.join(dataDir, configurationFile));
		pipeline = new Pipeline("unitPipeline", troilkattProperties, tfs);
		
		localRootDir = tmpDir;
		String hdfsPipelineMetaDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"), OsPath.join("meta", pipeline.name));
		hdfsStageMetaDir = OsPath.join(hdfsPipelineMetaDir, String.format("%03d-%s", 0, "executeSource"));
		hdfsStageTmpDir = OsPath.join(troilkattProperties.get("troilkatt.hdfs.root.dir"), "tmp");
		
		localVersion = OsPath.join(dataDir, "brainarray.html");
		args = pageURL + " " + filter;
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		source = new HREFSource("hrefource", args,
				"test/fileSource", "gz", 10, 
				localRootDir, hdfsStageMetaDir, hdfsStageTmpDir,
				pipeline); 
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testHREFSource() throws TroilkattPropertiesException, StageInitException, MalformedURLException {
		
		assertNotNull(source);
		assertEquals(new URL(pageURL), source.pageURL);
		
		String match = "http://brainarray.mbni.med.umich.edu/Brainarray/Database/CustomCDF/14.1.0/entrezg.download/DrosGenome1_Dm_ENTREZG_14.1.0.zip";
		Matcher matcher1 = source.pattern.matcher(match);
		assertTrue(matcher1.find());				
		String match2 = "entrezg.download/DrosGenome1_Dm_ENTREZG_14.1.0.zip";
		Matcher matcher2 = source.pattern.matcher(match2);
		assertTrue(matcher2.find());
		String noMatch = "http://brainarray.mbni.med.umich.edu/Brainarray/Database/CustomCDF/14.1.0/entrezg.download/bovinebtentrezg.db_14.1.0.zip";
		Matcher matcher3 = source.pattern.matcher(noMatch);
		assertFalse(matcher3.find());
	}

	@Test
	public void testGetHrefValues() throws IOException {
		ArrayList<String> hrefs = source.getHrefValues(localVersion);
		Collections.sort(hrefs);
		assertEquals(520, hrefs.size());
		
		String s1 = "entrezg.download/DrosGenome1_Dm_ENTREZG_14.1.0.zip";
		String s2 = "entrezg.download/bovinebtentrezg.db_14.1.0.zip";
		assertTrue(hrefs.contains(s1));
		assertTrue(hrefs.contains(s2));
	}

	@Test
	public void testDownloadTextFile() throws MalformedURLException, IOException {
		String localName = HREFSource.downloadTextFile(new URL(pageURL), tmpDir);
		fileCmp(localVersion, localName);
	}

	@Test
	public void testDownloadBinaryFile() throws MalformedURLException, IOException {
		String localName = HREFSource.downloadBinaryFile(new URL(pageURL), tmpDir);
		fileCmp(localVersion, localName);
	}

	@Test
	public void testRetrieve() throws StageException, IOException {
		ArrayList<String> metaFiles = new ArrayList<String>();
		ArrayList<String> logFiles = new ArrayList<String>();
		ArrayList<String> outputFiles = source.retrieve(metaFiles, logFiles, 525);
		
		assertEquals(2, outputFiles.size());
		Collections.sort(outputFiles);
		assertTrue(outputFiles.get(0).contains("DrosGenome1_Dm_ENTREZG_14.1.0.zip"));
		
		assertEquals(4, logFiles.size());
		Collections.sort(logFiles);
		assertTrue(logFiles.get(0).contains("allHrefs"));
		String[] hrefLines = FSUtils.readTextFile(logFiles.get(0));
		System.out.println("hrefLines: " + hrefLines.length);
		assertTrue(logFiles.get(1).contains("downloaded"));
		String[] downloadedLines = FSUtils.readTextFile(logFiles.get(1));
		assertEquals(2, downloadedLines.length);
		assertTrue(logFiles.get(2).contains("entrezg.asp"));
		assertTrue(logFiles.get(3).contains("toDownload"));
		String[] toDownloadLines = FSUtils.readTextFile(logFiles.get(3));
		assertEquals(2, toDownloadLines.length);
		
		assertEquals(0, metaFiles.size());
	}
}
