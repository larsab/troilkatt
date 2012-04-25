import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.LineReader;


public class TroilkattFS {
	/**
	 * Helper function to open a line reader for a file either in HDFS or a file 
	 * copied to the local file system (the file is stored in the specified input 
	 * directory). The latter is necessary for files compressed with a codec that
	 * is not supported by HDFS.
	 * 
	 * @param inputFilename HDFS filename to open
	 * @param inputDir directory on local FSwhere HDFS file is copied if it cannot 
	 * be read directly from HDFS
	 * @param tmpDir directory on local FS used for HDFS to local FS copy
	 * @param logDir log directory on local FS, also used during HDFS to local FS copy
	 * @return initialized LineReader, or null if the file could not be opened
	 * @throws IOException 
	 */
	public LineReader openLineReader(String inputFilename,
			String inputDir, String tmpDir, String logDir) throws IOException {	
		Path inputPath = new Path(inputFilename);
		String basename = getFilenameName(inputFilename);
		String compression = getFilenameCompression(inputFilename);
		long timestamp = getFilenameTimestamp(inputFilename);
		if ((basename == null) || (compression == null) || (timestamp == -1)) {				
			return null;
		}
		
		// Check if the compression format for the input file used is supported by Hadoop
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		CompressionCodec inputCodec = factory.getCodec(inputPath);
		
		// open a linereader either directly on the HDFs file or on a copy on local FS
		InputStream ins = null;
		FileInputStream fin = null;
		if (compression.equals("none")) {
			try {
				ins = hdfs.open(inputPath); 	
				return new LineReader(ins);
			} catch (FileNotFoundException e) {
				return null;
			}
		}
		else if (inputCodec != null) {
			try {
				ins = inputCodec.createInputStream(hdfs.open(inputPath));
				return new LineReader(ins);
			} catch (FileNotFoundException e) {
				return null;
			}
		}
		else {
			String localInputFilename = getFile(inputFilename, inputDir, tmpDir, logDir);
			if (localInputFilename == null) {
				return null;
			}
			// Read input file to local FS and open a stream to the local file				
			fin = new FileInputStream(new File(localInputFilename));				
			return new LineReader(fin);				
		}
	}
}
