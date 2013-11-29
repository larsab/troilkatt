package edu.princeton.function.troilkatt.source;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

/**
 * Mirror all GEO GSE (series) files
 */
public class GeoGSEMirror extends GeoGDSMirror {
	public static final String GSEftpDir = "/pub/geo/DATA/SOFT/by_series";

	/**
	 * Constructor
	 * 
	 * See superclass for arguments.
	 */
	public GeoGSEMirror(String name, String arguments, String outputDir,
			String compressionFormat, int storageTime, 
			String localRootDir, String tfsStageMetaDir, String tfsStageTmpDir,
			Pipeline pipeline)
			throws TroilkattPropertiesException, StageInitException {
		super(name, arguments, outputDir, compressionFormat, storageTime, localRootDir, tfsStageMetaDir, tfsStageTmpDir, pipeline, GSEftpDir);		
	}
	
	/**
	 * Helper function to download a GEO series file, unpack it, and save the unpacked files in tfs.
	 * 
	 * @param id of the series file to download
	 * @return tfs filename or null if file could not be downlaoded
	 * @throws StageException if an exception occurs during download, unpacking or tfs save
	 */
	@Override
	protected String downloadFile(FTPClient ftp, String id, long timestamp) throws StageException {
	
		logger.debug("Download series: " + id);
		String filename = id + "_family.soft.gz";
		String ftpFilename = OsPath.join(GSEftpDir, id + "/" + filename);
		
		// Make sure inputDir is empty before downloading, since it is later used to
		// unpack the files that are output of this stage
		OsPath.deleteAll(stageInputDir);
		OsPath.mkdir(stageInputDir);
	
		// Download file
		String outputFilename = OsPath.join(stageInputDir, OsPath.basename(filename));
		FileOutputStream fp = null;
		try {
			fp = new FileOutputStream(new File(outputFilename));
		} catch (FileNotFoundException e) {
			logger.fatal("Could not open output file: " + e.toString() + ": ", e);
			throw new StageException("Could not open output file: " + e);
		}
		
		try {
			if (ftp.setFileType(FTP.BINARY_FILE_TYPE) == false) {
				logger.warn("Could not set filetype to binary");
				fp.close();
				return null;
			}
			if (ftp.retrieveFile(ftpFilename, fp) == false) {
				logger.warn("Could not download file: " + filename + ": retrieve failed");				
				OsPath.delete(outputFilename);
				return null;
			}
			fp.close();
		} catch (IOException e) {
			logger.warn("Could not download file: " + filename, e);			
			OsPath.delete(outputFilename);
			return null;
		}
		 
		boolean fileUnpacked = unpackAll(stageInputDir, stageOutputDir);
		OsPath.delete(outputFilename);
		if (fileUnpacked == false) {
			logger.warn("Could not unpack file: " + outputFilename);			
			return null;
		}
		String[] unpackedFiles = OsPath.listdir(stageOutputDir, logger);
	
		for (String u: unpackedFiles) {
			if (u.endsWith(".SOFT") || u.endsWith(".soft")) {
				String tfsFilename = tfs.putLocalFile(u, tfsOutputDir, stageTmpDir, stageLogDir, compressionFormat, timestamp);				
				OsPath.delete(u);
				if (tfsFilename != null) {
					// File was successfully downloaded
					return tfsFilename;
				}
				else {
					logger.fatal("Could not copy downloaded file to tfs");					
					throw new StageException("Could not copy downloaded file to tfs");
				}
			}
		}		
		
		// File could not be downloaded
		return null;
	}
}
