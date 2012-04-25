package edu.princeton.function.troilkatt;

import static org.junit.Assert.*;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.SAXException;

import edu.princeton.function.troilkatt.fs.OsPath;

public class TroilkattPropertiesTest extends TestSuper {
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {		
	}
	

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testTroilkattProperties() throws TroilkattPropertiesException {
		TroilkattProperties p = new TroilkattProperties(OsPath.join(dataDir, configurationFile));
		assertNotNull(p.filename);
		// The constructor has a call to a function that verifies the configuration file
	}
	
	// Invalid configuration file
	@Test(expected=TroilkattPropertiesException.class)
	public void testTroilkattProperties2() throws TroilkattPropertiesException {
		TroilkattProperties p = new TroilkattProperties(OsPath.join(dataDir, "invalidConfig1.xml"));
		assertNotNull(p.filename);
		// The constructor has a call to a function that verifies the configuration file
	}
	
	// Invalid configuration property
	@Test(expected=TroilkattPropertiesException.class)
	public void testTroilkattProperties3() throws TroilkattPropertiesException {
		TroilkattProperties p = new TroilkattProperties(OsPath.join(dataDir, "invalidConfig2.xml"));
		assertNotNull(p.filename);
		// The constructor has a call to a function that verifies the configuration file
	}
	
	// Missing property
	@Test(expected=TroilkattPropertiesException.class)
	public void testTroilkattProperties4() throws TroilkattPropertiesException {
		TroilkattProperties p = new TroilkattProperties(OsPath.join(dataDir, "invalidConfig3.xml"));
		assertNotNull(p.filename);
		// The constructor has a call to a function that verifies the configuration file
	}
	
	// Unsupported propery
	@Test(expected=TroilkattPropertiesException.class)
	public void testTroilkattProperties5() throws TroilkattPropertiesException {
		TroilkattProperties p = new TroilkattProperties(OsPath.join(dataDir, "invalidConfig4.xml"));
		assertNotNull(p.filename);
		// The constructor has a call to a function that verifies the configuration file
	}

	// Non-existing configuration file
	@Test(expected=TroilkattPropertiesException.class)
	public void testTroilkattProperties6() throws TroilkattPropertiesException {
		TroilkattProperties p = new TroilkattProperties(OsPath.join(dataDir, "nonExistingFile.xml"));
		assertNotNull(p.filename);
		// The constructor has a call to a function that verifies the configuration file
	}
	
	@Test
	public void testGet() throws SAXException, IOException, ParserConfigurationException, TroilkattPropertiesException {
		TroilkattProperties p = new TroilkattProperties(OsPath.join(dataDir, configurationFile));
		assertEquals("/home/larsab/troilkatt2/test-tmp/data", p.get("troilkatt.localfs.dir"));
		assertEquals("/user/larsab/troilkatt", p.get("troilkatt.hdfs.root.dir"));
		assertEquals("24", p.get("troilkatt.update.interval.hours"));
	}
	
	// Invalid property
	@Test(expected=TroilkattPropertiesException.class)
	public void testGet2() throws SAXException, IOException, ParserConfigurationException, TroilkattPropertiesException {
		TroilkattProperties p = new TroilkattProperties(OsPath.join(dataDir, configurationFile));
		assertNotNull(p.get("foo.bar.baz"));
	}

	@Test
	public void testSet() throws SAXException, IOException, ParserConfigurationException, TroilkattPropertiesException {
		TroilkattProperties p = new TroilkattProperties(OsPath.join(dataDir, configurationFile));
		p.set("troilkatt.update.interval.hours", "1");
		assertEquals("1", p.get("troilkatt.update.interval.hours"));
		p.set("foo.bar", "baz");
		assertEquals("baz", p.get("foo.bar"));
	}

	@Test
	public void testGetAll() throws SAXException, IOException, ParserConfigurationException, TroilkattPropertiesException {
		TroilkattProperties p = new TroilkattProperties(OsPath.join(dataDir, configurationFile));
		assertEquals(p.properties, p.getAll());
	}

	@Test
	public void testGetConfigFile() throws SAXException, IOException, ParserConfigurationException, TroilkattPropertiesException {
		TroilkattProperties p = new TroilkattProperties(OsPath.join(dataDir, configurationFile));
		assertEquals(configurationFile, OsPath.basename(p.getConfigFile()));
	}

}
