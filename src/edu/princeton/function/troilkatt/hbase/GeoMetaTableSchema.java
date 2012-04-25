package edu.princeton.function.troilkatt.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * Hbase table that holds meta-data extracted from the SOFT file and calculated fields.
 * 
 * family "meta" (extracted from SOFT/PCL file):
 * - id
 * - title
 * - date
 * - pmid
 * - description
 * - organisms
 * - platformIDs
 * - platformTitles
 * - rowCounts
 * - sampleIDs: list of GSM sample IDs. These are separated by newlines.
 * - sampleTitles: list of sample titles
 * - channelCounts
 * - valueTypes
 * 
 * family "calculated": (calculated from PCL file expression values)
 * - min
 * - max
 * - mean
 * - numNeg
 * - numPos		
 * - numZero 
 * - numMissing
 * - numTotal
 * - channels
 * - logged: 1 if expression values have been log transformed, 0 if not
 * - zerosAreMVs: 1 if zeros are missing values, zero otherwise
 * - cutoff: missing value cutoff. Can also be "NaN"
 * 
 * family "processed"
 * - samplesIDs-overlapRemoved: list of GSM sample IDs included after overlapping samples
 *   have been removed. If entire dataset is deleted, this field is set to "none". This 
 *   field is not set if overlap calculation is not done for this dataset.
 * 
 * family "files":
 * - softFilename: filename of the soft file for this dataset/series
 * - pclFilename: filename of soft2pcl output file
 * 
 * family "other":
 */
public class GeoMetaTableSchema extends TroilkattTable {

	/**
	 * Constructor.
	 */
	public GeoMetaTableSchema() {
		tableName = "troilkatt-geo-meta";
		
		colFams = new String[5];
		colFams[0] = "meta";
		colFams[1] = "calculated";
		colFams[2] = "processed";
		colFams[3] = "files";
		colFams[4] = "other";
		
		// Keep a couple of versions for debugging and test purposes
		maxVersions = 5;
	}
	
	/**
	 * Read an "info" value from the GEO meta table
	 * 
	 * @param gid dataset/series identifier used as row key in the GEO meta table
	 * @param columnQualifier the field to read (the column family is automatically added)
	 * @param metaTable initialized GEO meta data handle
	 * @param logger optional logger. If null, no error messages are written
	 * @return value or null if the value could not be read
	 * @throws IOException 
	 */
	public static String getInfoValue(HTable metaTable, String gid, String columnQualifier,
			Logger logger) throws IOException {
		return getValue(metaTable, gid, "calculated", columnQualifier, logger);			
	}
	
	/**
	 * Read a "meta" value from the GEO meta table
	 * 
	 * @param gid dataset/series identifier used as row key in the GEO meta table
	 * @param columnQualifier the field to read (the column family is automatically added)
	 * @param metaTable initialized GEO meta data handle
	 * @param logger optional logger. If null, no error messages are written
	 * @return value or null if the value could not be read
	 * @throws IOException 
	 */
	public static String getMetaValue(HTable metaTable, String gid, String columnQualifier,
			Logger logger) throws IOException {
		return getValue(metaTable, gid, "meta", columnQualifier, logger);			
	}
	
	/**
	 * Read a value from the GEO meta table
	 * 
	 * @param gid dataset/series identifier used as row key in the GEO meta table
	 * @param columnFamily the column to read
	 * @param columnQualifier the field to read
	 * @param metaTable initialized GEO meta data handle
	 * @param logger optional logger. If null, no error messages are written
	 * @return value or null if the value could not be read
	 */
	public static String getValue(HTable metaTable, String gid, 
			String columnFamily, String columnQualifier,
			Logger logger) throws IOException {		
		Get get = new Get(Bytes.toBytes(gid));	
		byte[] fam = Bytes.toBytes(columnFamily);			
		get.addColumn(fam, Bytes.toBytes(columnQualifier));
		
		Result result;
		try {
			result = metaTable.get(get);
		} catch (IOException e) {
			if (logger != null) {
				logger.fatal("IOException during get row: " + gid + " with column family: " + columnFamily, e);
			}
			return null;
		}
		
		if (result == null) {
			if (logger != null) {
				logger.error("Could not get meta data for row: " + gid);
			}			
			return null;
		}						
		
		byte[] valBytes = result.getValue(fam, Bytes.toBytes(columnQualifier));
		if (valBytes == null) {
			if (logger != null) {
				logger.error("Null value for " + columnFamily + ":" + columnQualifier + " in row: " + gid);
			}
			return null;
		}
		
		String val = Bytes.toString(valBytes);
		return val;	
	}
}
