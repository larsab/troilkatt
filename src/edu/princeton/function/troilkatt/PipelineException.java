package edu.princeton.function.troilkatt;

/**
 * Exception thrown if a pipeline configuration file cannot be parsed.
 */
public class PipelineException extends Exception {
	private static final long serialVersionUID = -1492642222150096754L;
	
	public PipelineException(String msg) {
		super(msg);
	}
}
