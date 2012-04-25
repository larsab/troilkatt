package edu.princeton.function.troilkatt.tools;

/**
 * Exception thrown if a stage process() fails.
 */
public class ParseException extends Exception {
	private static final long serialVersionUID = 1L;

	public ParseException(String msg) {
		super(msg);
	}

}
