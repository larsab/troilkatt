package edu.princeton.function.troilkatt.pipeline;

/**
 * Exception thrown if a stage process() fails.
 */
public class StageInitException extends Exception {
	private static final long serialVersionUID = 1L;

	public StageInitException(String msg) {
		super(msg);
	}

}
