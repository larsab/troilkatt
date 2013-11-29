package edu.princeton.function.troilkatt.source;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import edu.princeton.function.troilkatt.Pipeline;
import edu.princeton.function.troilkatt.TroilkattPropertiesException;
import edu.princeton.function.troilkatt.fs.FSUtils;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageInitException;

/**
 * Download all files linked to from a HTML page that match a set of patterns. 
 * 
 */
public class HREFSource extends Source {

	// Link to page with URL's to download
	protected URL pageURL;	
	// Regular expression pattern to match
	protected Pattern pattern;
	
	/**
	 * Constructor.
	 * 
	 * For argument description, refer to superclass.
	 *
	 * @param arguments URL pattern1 pattern2...patternN
	 */
	public HREFSource(String name, String arguments, String outputDir,
			String compressionFormat, int storageTime, String localRootDir,
			String tfsStageMetaDir, String tfsStageTmpDir, Pipeline pipeline)
			throws TroilkattPropertiesException, StageInitException {
		super(name, arguments, outputDir, compressionFormat, storageTime,
				localRootDir, tfsStageMetaDir, tfsStageTmpDir, pipeline);		
		
		String[] parts = arguments.split(" ");
		if (parts.length < 1) {
			logger.fatal("Source page URL not specified in arguments");
			throw new StageInitException("Source page URL not specified in arguments");
		}
		else if (parts.length < 2) {
			logger.info("No pattern specified for URLs to match.");
		}
		
		try {
			pageURL = new URL(parts[0]);
		} catch (MalformedURLException e1) {
			logger.fatal("Invalid source page URL: " + parts[0]);
			throw new StageInitException("Invalid source page URL: " + parts[0]);
		}
				
		try {
			logger.info("Pattern to match: " + parts[1]);
			pattern =  Pattern.compile(parts[1]);
		} catch (PatternSyntaxException e) {
			logger.fatal("Invalid filter pattern: " + parts[1]);
			throw new StageInitException("Invalid filter pattern: " + parts[1]);
		}
				
	}
	
	/**
	 * Download the source page and the files linked from the source file
	 * 
	 * @param metaFiles ignored
	 * @param logFiles list for storing log files produced by this step
	 * @return list of output files in TFS
	 * @throws StageException 
	 */
	@Override
	protected ArrayList<String> retrieve(ArrayList<String> metaFiles, 
			ArrayList<String> logFiles, long timestamp) throws StageException {
		
		ArrayList<String> outputFiles = new ArrayList<String>();
		logger.info("Retrieve");
		
		/*
		 * Download source file
		 */
		
		// The page specified by the URL is saved as this file
		String sourceFilename = null;
		logger.info("Download " + pageURL.toString() + " to dir: " + stageLogDir);
		try {
			sourceFilename = downloadTextFile(pageURL, stageLogDir);
			logFiles.add(sourceFilename);
		} catch (IOException e) {
			logger.fatal("Could not download page specified by URL: " + e.getMessage());
			throw new StageException("Could not download page specified by URL: " + e.getMessage());
		}
		
		/*
		 * Parse HTML to find all href tags
		 */
		ArrayList<String> urls = null;
		try {			
			urls = getHrefValues(sourceFilename);			
		} catch (IOException e) {
			logger.fatal("Could not read from recently downloaded file: " + e.getMessage());
			throw new StageException("Could not read from recently downloaded file: " + sourceFilename);
		}
		logger.info("Found " + urls.size() + " URLs");
			
		String logFilename = OsPath.join(stageLogDir, "allHrefs");
		try {			
			FSUtils.writeTextFile(logFilename, urls.toArray(new String[urls.size()]));
		} catch (IOException e) {
			logger.error("Could not write logfile: " + logFilename);
		}
		logFiles.add(logFilename);
		
		/*
		 * Match URLs to provided patterns
		 */
		ArrayList<String> toDownload = new ArrayList<String>();		
		for (String u: urls) {			
			Matcher matcher = pattern.matcher(u);
			if (matcher.find()) {				
				toDownload.add(u);
			}			
		}
		logger.info("Of " + urls.size() + " URLs, " + toDownload.size() + " match pattern");
		
		logFilename = OsPath.join(stageLogDir, "toDownload");
		try {			
			FSUtils.writeTextFile(logFilename, toDownload.toArray(new String[toDownload.size()]));
		} catch (IOException e) {
			logger.error("Could not write logfile: " + logFilename);
		}
		logFiles.add(logFilename);
		
		/*
		 * Attempt to download retrieved URL's that match pattern, and save
		 * downloaded files in TFS
		 */		
		try {
			URI cwd = pageURL.toURI();		
			for (String u: toDownload) {
				logger.info("Download file: " + u);
				URI uri = new URI(u);
				URL url = null;
				if (uri.isAbsolute()) {
					url = new URL(u);
				}
				else {
					URI absUri = cwd.resolve(uri);
					url = absUri.toURL();
				}

				String fn = downloadBinaryFile(url, stageOutputDir);
				String tfsFilename = tfs.putLocalFile(fn, tfsOutputDir, stageTmpDir, stageLogDir, compressionFormat, timestamp);
				if (tfsFilename != null) {
					outputFiles.add(tfsFilename);
				}
				else {
					logger.warn("Could not save file in TFS: " + fn);
				}
				OsPath.delete(fn);				
			}
		} catch (MalformedURLException e) {
			logger.warn("Invalid URL in href: " + e.getMessage());
		} catch (IOException e) {
			logger.warn("Could not download file: " + e.getMessage());
		} catch (URISyntaxException e) {
			logger.warn("Invalid URI in href: " + e.getMessage());
		}
		logger.info("Downloaded: " + outputFiles.size() + " files");
		
		logFilename = OsPath.join(stageLogDir, "downloaded");
		try {			
			FSUtils.writeTextFile(logFilename, outputFiles.toArray(new String[toDownload.size()]));
		} catch (IOException e) {
			logger.error("Could not write logfile: " + logFilename);
		}
		logFiles.add(logFilename);
		
		return outputFiles;
	}
	
	/**
	 * Parse HTML file to find href tags
	 * 
	 * @param filename file on local filesystem to parse
	 * @return list of href values
	 * @throws IOException 
	 */
	/**
	 * @param filename
	 * @return list of URL's to download
	 * @throws IOException
	 */
	public static ArrayList<String> getHrefValues(String filename) throws IOException {
		
		FileReader fin = new FileReader(filename);
	    Scanner src = new Scanner(fin);
	    
	    Pattern hrefPattern = Pattern.compile("[^<]*(<a href=\"([^\"]+)\">([^<]+)<\\/a>)");	   
	    
	    //Matcher matcher = pattern.matcher("<a href=\"http://brainarray.mbni.med.umich.edu/Brainarray/default.asp\">Home</a> &gt;");
	    //if (matcher.find()) {
	    //	System.out.println("Found");
	    //}
	    
	    ArrayList<String> urls = new ArrayList<String>();
	    while (true) {
	    	String aTag = src.findWithinHorizon(hrefPattern, 1024*1024);
	    	if (aTag == null) {
	    		break;
	    	}	    	
	    	
	    	int hrefStart = aTag.indexOf("href");
			if (hrefStart == -1) {
				System.err.println("Could not find href in: " + aTag);
				continue;
			}
			int linkStart = aTag.indexOf("\"", hrefStart + 4) + 1;
			if (linkStart == 0) {
				System.err.println("Could not find start of string");
				continue;
			}
			int linkEnd = aTag.indexOf("\"", linkStart);
			if (linkEnd == -1) {
				System.err.println("Could not find end of string");
				continue;
			}
			String downloadURL = aTag.substring(linkStart, linkEnd);
			urls.add(downloadURL);
	    }
	    
		src.close();
		
	    return urls;
	}
	
	/**
	 * Download a text file specified by an URL
	 * 
	 * @param srcURL file to download
	 * @param dstName destination file
	 * @return filename filename on local file system
	 */
	public static String downloadTextFile(URL src, String dstDir) throws IOException {
		String basename = OsPath.basename(src.getFile());
		String dstName = OsPath.join(dstDir, basename);
		
		BufferedReader in = new BufferedReader(
				new InputStreamReader(src.openStream()));
		PrintWriter out = new PrintWriter(new FileWriter(dstName));
		
		String inputLine;
		while ((inputLine = in.readLine()) != null) {
		    out.println(inputLine);
		}
		
		in.close();
		out.close();
		
		return dstName;
	}
	
	/**
	 * Download a text file specified by an URL
	 * 
	 * @param srcURLURL of file to download
	 * @param dstName destination file
	 * @return filename filename on local file system
	 */
	public static String downloadBinaryFile(URL src, String dstDir) throws IOException {
		String basename = OsPath.basename(src.getFile());
		String dstName = OsPath.join(dstDir, basename);
		
		InputStream in = src.openStream();
		OutputStream os = new FileOutputStream(dstName);
		
		org.apache.commons.net.io.Util.copyStream(in, os);
		
		in.close();
		os.close();
		
		return dstName;
	}
	
	/**
	 * Debug and development main function
	 * 
	 * @param args not used
	 * @throws IOException 
	 * @throws URISyntaxException 
	 */
	public static void main(String args[]) throws IOException, URISyntaxException {
		URL pageURL = new URL("http://brainarray.mbni.med.umich.edu/Brainarray/Database/CustomCDF/13.0.0/entrezg.asp");
		String outputDir = "C:\\Users\\larsab\\papers12\\incremental-updates\\imp\\brainarray\\2010-06";
		//Pattern pattern =  Pattern.compile(".*_Mm_ENTREZG_.*\\.zip");
		Pattern pattern =  Pattern.compile(".*mmentrezgcdf_.*\\.tar.gz");
				
		String sourceFilename = HREFSource.downloadTextFile(pageURL, outputDir);

		ArrayList<String> urls = HREFSource.getHrefValues(sourceFilename);			
		
		ArrayList<String> toDownload = new ArrayList<String>();		
		for (String u: urls) {			
			Matcher matcher = pattern.matcher(u);
			if (matcher.find()) {				
				toDownload.add(u);
			}
		}
				
		URI cwd = pageURL.toURI();
		for (String u: toDownload) {
			URI uri = new URI(u);
			URL url = null;			
			if (uri.isAbsolute()) {
				url = new URL(u);
			}
			else {
				URI absUri = cwd.resolve(uri);
				url = absUri.toURL();
			}
			
			HREFSource.downloadBinaryFile(url, outputDir);
		}		
	}
}
