package edu.princeton.function.troilkatt.pipeline;

/**
 * Exception thrown if a stage process() fails.
 */
public class StageException extends Exception {
	private static final long serialVersionUID = 1L;

	public StageException(String msg) {
		super(msg);
	}

}
