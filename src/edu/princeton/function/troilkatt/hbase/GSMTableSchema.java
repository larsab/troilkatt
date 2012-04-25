package edu.princeton.function.troilkatt.hbase;

/**
 * Schema:
 *
 * key: dataset/ series ID (GSE ID or GDS ID)
 *
 * family "in":
 * - GDS: list of GSM samples for datasets
 * - GSE: list of GSM samples for series
 * 
 * family "meta":
 * 
 */
public class GSMTableSchema extends TroilkattTable {
	public GSMTableSchema() {
		tableName = "troilkatt-gsm2gid";
		
		colFams = new String[2];
		colFams[0] = "in";
		colFams[1] = "meta";
		
		// Keep a couple of versions for debugging and test purposes
		maxVersions = 5;
	}
}
