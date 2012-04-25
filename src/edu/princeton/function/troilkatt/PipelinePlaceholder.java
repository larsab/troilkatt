/**
 * 
 */
package edu.princeton.function.troilkatt;

import java.io.IOException;

import edu.princeton.function.troilkatt.fs.TroilkattFS;

/**
 * Pipeline for MapReduce tasks and unit tests that only need to use a pipeline
 * object as place holder for various variables.
 */
public class PipelinePlaceholder extends Pipeline {

	public PipelinePlaceholder(String name,
			TroilkattProperties troilkattProperties, TroilkattFS tfs)
			throws PipelineException, TroilkattPropertiesException {
		super(name, troilkattProperties, tfs);		
	}

	/**
	 * Invalid function for this class
	 */
	@Override
	public boolean update(long timestamp, TroilkattStatus status) throws IOException {
		throw new RuntimeException("Update method called in PipelinePlaceholder object");
	}


	/**
	 * Invalid function for this class
	 */
	@Override
	public boolean recover(long timestamp, TroilkattStatus status) throws IOException {
		throw new RuntimeException("Recover method called in PipelinePlaceholder object");
	}
}
