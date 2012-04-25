package edu.princeton.function.troilkatt.hbase;

/**
 * Exception thrown if a stage process() fails.
 */
public class HbaseException extends Exception {
	private static final long serialVersionUID = 1L;

	public HbaseException(String msg) {
		super(msg);
	}

}
