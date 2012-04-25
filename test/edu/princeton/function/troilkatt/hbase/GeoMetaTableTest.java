package edu.princeton.function.troilkatt.hbase;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.princeton.function.troilkatt.hbase.GeoMetaTableSchema;

public class GeoMetaTableTest {
	protected static GeoMetaTableSchema schema;
	protected static HTable metaTable;
	protected static Logger testLogger;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		Configuration hbConf = HBaseConfiguration.create();
		schema = new GeoMetaTableSchema();
		metaTable = schema.openTable(hbConf, true);
		
		testLogger = Logger.getLogger("test");
		
		String dsetID = "unittest";
		if (GeoMetaTableSchema.getValue(metaTable, dsetID, "meta", "id", testLogger) == null) {
			// Add row to meta table
			Put update = new Put(Bytes.toBytes(dsetID), 31278);
			byte[] metaFamily = Bytes.toBytes("meta");			
			update.add(metaFamily, Bytes.toBytes("id"), Bytes.toBytes("unit-test"));
			update.add(metaFamily, Bytes.toBytes("organism"), Bytes.toBytes("Notareal Organism"));
			update.add(metaFamily, Bytes.toBytes("sampleIDs"), Bytes.toBytes("sample1\nsample2\nsample3\nsample4\nsample5\nsample6\nsample7"));

			byte[] infoFamily = Bytes.toBytes("calculated");			
			update.add(infoFamily, Bytes.toBytes("logged"), Bytes.toBytes("0"));
			update.add(infoFamily, Bytes.toBytes("zerosAreMVs"), Bytes.toBytes("1"));
			update.add(infoFamily, Bytes.toBytes("cutoff"), Bytes.toBytes("NaN"));

			byte[] processedFamily = Bytes.toBytes("processed");			
			update.add(processedFamily, Bytes.toBytes("sampleIDs-overlapRemoved"), Bytes.toBytes("sample1\nsample2\nsample3"));

			byte[] filesFamily = Bytes.toBytes("files");			
			update.add(filesFamily, Bytes.toBytes("softFilename"), Bytes.toBytes("/foo/bar/baz.soft.31278.bz"));

			metaTable.put(update);
		}
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
	public void testGeoMetaTable() {
		GeoMetaTableSchema t = new GeoMetaTableSchema();
		assertEquals("troilkatt-geo-meta", t.tableName);
		assertEquals(5, t.colFams.length);
		assertEquals("meta", t.colFams[0]);
		assertEquals("calculated", t.colFams[1]);
		assertEquals("processed", t.colFams[2]);
		assertEquals("files", t.colFams[3]);
		assertEquals("other", t.colFams[4]);
	}
	
	@Test
	public void testGetValue() throws IOException {
		assertEquals("unit-test", GeoMetaTableSchema.getValue(metaTable, "unittest", "meta", "id", testLogger));
		assertEquals("Notareal Organism", GeoMetaTableSchema.getValue(metaTable, "unittest", "meta", "organism", testLogger));
		assertEquals("0", GeoMetaTableSchema.getValue(metaTable, "unittest", "calculated", "logged", testLogger));
		assertEquals("sample1\nsample2\nsample3", GeoMetaTableSchema.getValue(metaTable, "unittest", "processed", "sampleIDs-overlapRemoved", testLogger));
		assertEquals("/foo/bar/baz.soft.31278.bz", GeoMetaTableSchema.getValue(metaTable, "unittest", "files", "softFilename", testLogger));
		
		// invalid row
		assertNull(GeoMetaTableSchema.getValue(metaTable, "non-existing-row", "meta", "id", testLogger));
		// invalid fam
		assertNull(GeoMetaTableSchema.getValue(metaTable, "unittest", "invalid-fam", "id", testLogger));
		// invalid qual
		assertNull(GeoMetaTableSchema.getValue(metaTable, "unittest", "meta", "invalid-qual", testLogger));		
		// null logger
		assertEquals("unit-test", GeoMetaTableSchema.getValue(metaTable, "unittest", "meta", "id", null));
		assertNull(GeoMetaTableSchema.getValue(metaTable, "unittest", "invalid-fam", "id", null));
	}
	
	@Test
	public void getInfoValuesTest() throws IOException {
		assertEquals("0", GeoMetaTableSchema.getInfoValue(metaTable, "unittest", "logged", testLogger));
		assertEquals("NaN", GeoMetaTableSchema.getInfoValue(metaTable, "unittest", "cutoff", testLogger));			
	}

}
