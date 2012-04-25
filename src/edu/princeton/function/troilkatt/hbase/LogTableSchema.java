package edu.princeton.function.troilkatt.hbase;

public class LogTableSchema extends TroilkattTable {
	/**
	 * Constructor.
	 * 
	 * @param naem Hbase table name
	 */
	public LogTableSchema(String name) {
		tableName = name;
		
		colFams = new String[4];
		colFams[0] = "out";   // files with out extension:                      column key: filename
		colFams[1] = "error"; // files with error extension:                    column key: filename
		colFams[2] = "log";   // files with log extension:                      column key: filename
		colFams[3] = "other"; // files that do not match any of above criteria: column key: filename
	}
}
