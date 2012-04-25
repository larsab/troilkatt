package edu.princeton.function.troilkatt.fs;

/**
 * Exception thrown if a stage process() fails.
 */
public class TroilkattFSException extends Exception {
	private static final long serialVersionUID = 1L;

	public TroilkattFSException(String msg) {
		super(msg);
	}

}
