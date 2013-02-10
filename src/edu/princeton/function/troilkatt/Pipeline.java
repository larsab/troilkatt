package edu.princeton.function.troilkatt;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;
import org.apache.log4j.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import edu.princeton.function.troilkatt.fs.LogTable;
import edu.princeton.function.troilkatt.fs.LogTableHbase;
import edu.princeton.function.troilkatt.fs.LogTableTar;
import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.fs.TroilkattFS;
import edu.princeton.function.troilkatt.pipeline.Stage;
import edu.princeton.function.troilkatt.pipeline.StageException;
import edu.princeton.function.troilkatt.pipeline.StageFactory;
import edu.princeton.function.troilkatt.pipeline.StageInitException;
import edu.princeton.function.troilkatt.sink.Sink;
import edu.princeton.function.troilkatt.sink.SinkFactory;
import edu.princeton.function.troilkatt.source.Source;
import edu.princeton.function.troilkatt.source.SourceFactory;

/**
 * Place holder for a Troilkatt pipeline.
 */
public class Pipeline {
	// Unique name for pipeline
	public String name;
	private Logger logger;

	// Shared among all stages
	public TroilkattProperties troilkattProperties; 	
	public TroilkattFS tfs;
	public LogTable logTable;

	// Global variables specified in the configuration file and set in createPipeline()
	protected Source source = null;
	protected Vector<Stage> pipeline = new Vector<Stage>();
	protected Sink sink = null;
	
	// Directory on local FS for temporary files
	public String localTmpDir;
	
	/**
	 * Constructor
	 *
	 * @param name unique name for dataset
	 * @param datasetFile filename of XML file with the pipeline specification
	 * @param troilkattProperties troilkatt properties object
	 * @param tfs Troilkatt FS handle
	 * @throws IOException if a directory on the local FS or HDFS could not be created
	 * @throws PipelineException if pipeline configuration XML file could not be parsed
	 * @throws TroilkattPropertiesException 
	 * @throws StageInitException 
	 */
	public Pipeline(String name, String datasetFile, 
			TroilkattProperties troilkattProperties, TroilkattFS tfs) throws PipelineException, TroilkattPropertiesException, StageInitException {

		this.name = name;
		this.logger = Logger.getLogger("troilkatt.pipeline-" + name); 
		logger.debug("Initialize pipeline: " + name);
		this.troilkattProperties = troilkattProperties;		
		this.tfs = tfs;
		
		// Create tmp directory on local filesystem if needed
		localTmpDir = OsPath.join(troilkattProperties.get("troilkatt.localfs.dir"), name);		
		if (! OsPath.isdir(localTmpDir)) {
			if (! OsPath.mkdir(localTmpDir)) {
				logger.fatal("Could not create directory: " + localTmpDir);
				throw new PipelineException("mkdir " + localTmpDir + " failed");
			}
		}
		
		String tfsRootDir;
		String persistentStorage = troilkattProperties.get("troilkatt.persistent.storage");
		if (persistentStorage.equals("hadoop")) {
			this.logTable = new LogTableHbase(name);
			tfsRootDir = troilkattProperties.get("troilkatt.hdfs.root.dir");		
		}
		else if (persistentStorage.equals("nfs")) {
			String sgeDir = troilkattProperties.get("troilkatt.globalfs.sge.dir");
			String localLogDir = OsPath.join(troilkattProperties.get("troilkatt.localfs.log.dir"), "logtar");
			if (! OsPath.mkdir(localLogDir)) {
				logger.fatal("Could not create directory: " + localLogDir);
				throw new PipelineException("mkdir " + localLogDir + " failed");
			}
			this.logTable = new LogTableTar(name, tfs, OsPath.join(sgeDir, "log"), localLogDir, localTmpDir);
			tfsRootDir = troilkattProperties.get("troilkatt.nfs.root.dir");
		}
		else {
			logger.fatal("Invalid valid for persistent storage");
			throw new PipelineException("Invalid valid for persistent storage");
		}
		
		// Create directories on HDFS if needed
		String hdfsDatadir = OsPath.join(tfsRootDir, "data/");		
		String hdfsLogdir = OsPath.join(tfsRootDir, "log/" + name);
		String hdfsMetadir = OsPath.join(tfsRootDir, "meta/" + name);
		String hdfsGlobalMetadir = OsPath.join(tfsRootDir, "global-meta");
		try {
			// The tfs.mkdir function will check if a directory exists before the mkdir call.
			// If a directory exists the function returns without a warning or exceptions
			tfs.mkdir(hdfsDatadir);
			tfs.mkdir(hdfsLogdir);
			tfs.mkdir(hdfsMetadir);
			tfs.mkdir(hdfsGlobalMetadir);			
		} catch (IOException e) {
			logger.fatal("Could not create HDFS directories: " + e);
			throw new PipelineException("Could not create HDFS directories");
		}
		
		createPipeline(datasetFile);

		logger.info("Data set initialized with the following arguments:");        
		logger.info("\tsource: " + source.stageName);        
		logger.info("\tstages");
		for (Stage s: pipeline) {
			logger.info("\t\t: " + s.stageName);
		}
		logger.info("\tsink: " + sink.stageName);
	}


	/**
	 * Constructor used by MapReduce tasks and unit tests. In this case the pipeline is used
	 * only as a place holder for variables. Note! that this function should not be called
	 * directly. Instead the PipelinePlacholder subclass should be used that has an implicit
	 * call to this constructor
	 * 
	 * @param name pipeline name
	 * @param troilkattProperties initialized properties object
	 * @param tfs initialized file system handle
	 * @param pipelineTmpDir temporary directory used by MapReduce jon
	 * @throws PipelineException 
	 * @throws TroilkattPropertiesException 
	 */
	public Pipeline(String name, TroilkattProperties troilkattProperties, TroilkattFS tfs) throws PipelineException, TroilkattPropertiesException {
		this.name = name;
		logger = Logger.getLogger("troilkatt.pipeline-" + name); 
		this.troilkattProperties = troilkattProperties;
		this.tfs = tfs;
		
		// Create tmp directory on local filesystem if needed
		localTmpDir = OsPath.join(troilkattProperties.get("troilkatt.localfs.dir"), name);		
		if (! OsPath.isdir(localTmpDir)) {
			if (! OsPath.mkdir(localTmpDir)) {
				logger.fatal("Could not create directory: " + localTmpDir);
				throw new PipelineException("mkdir " + localTmpDir + " failed");
			}
		}

		String persistentStorage = troilkattProperties.get("troilkatt.persistent.storage");
		if (persistentStorage.equals("hadoop")) {
			this.logTable = new LogTableHbase(name);
		}
		else if (persistentStorage.equals("nfs")) {
			String sgeDir = troilkattProperties.get("troilkatt.globalfs.sge.dir");
			String localLogDir = OsPath.join(troilkattProperties.get("troilkatt.localfs.log.dir"), "logtar");
			if (! OsPath.mkdir(localLogDir)) {
				logger.fatal("Could not create directory: " + localLogDir);
				throw new PipelineException("mkdir " + localLogDir + " failed");
			}
			this.logTable = new LogTableTar(name, tfs, OsPath.join(sgeDir, "log"), localLogDir, localTmpDir);
		}
		else {
			logger.fatal("Invalid valid for persistent storage");
			throw new PipelineException("Invalid valid for persistent storage");
		}
		
		// Create tmp file on local filesystem if needed
		// Create tmp file on local filesystem if needed
		localTmpDir = OsPath.join(troilkattProperties.get("troilkatt.localfs.dir"), name);		
		if (! OsPath.isdir(localTmpDir)) {
			if (! OsPath.mkdir(localTmpDir)) {
				logger.fatal("Could not create directory: " + localTmpDir);
				throw new PipelineException("mkdir " + localTmpDir + " failed");
			}
		}
	}


	/**
	 * Update pipeline with new data from source.
	 *
	 * @param timestamp current timestamp added to generated files
	 * @param status TroilkattStatus handle
	 * @return true if pipeline was successfully, false if an error occured
	 * @throws IOException if status file could not be updated
	 */
	public boolean update(long timestamp, TroilkattStatus status) throws IOException {
		logger.fatal("\nUpdate pipeline " + name + " at " + timestamp);
	
		try {
			// Retrieve files to process		
			status.setStatus(source.stageName, timestamp, "start");
			ArrayList<String> inputFiles = source.retrieve2(timestamp);
			status.setStatus(source.stageName, timestamp, "done");
	
			logger.info("Retrieved: " + inputFiles.size());
			
			// Process data        
			for (Stage s: pipeline) {
				if ((s == source) || (s == sink)) {
					continue;
				}
				status.setStatus(s.stageName, timestamp, "start");
				inputFiles = s.process2(inputFiles, timestamp);			
				status.setStatus(s.stageName, timestamp, "done");
				
				logger.info("Processed: " + inputFiles.size());
			}
	
			// Execute sink
			status.setStatus(sink.stageName, timestamp, "start");
			sink.sink2(inputFiles, timestamp);
			status.setStatus(sink.stageName, timestamp, "done");
			
			logger.info("Sunk: " + inputFiles.size());
			
			return true;
		} catch (StageException e) {
			logger.error("Could not process a pipeline stage: " + e.toString());	
			e.printStackTrace();
			return false;
		} 
	}


	/**
	 * Recover from a previously failed execution by updating the stages that did not complete
	 * in the previous iteration.
	 *
	 * @param timestamp for the iteration that did not complete
	 * @param status TroilkattStatus handle
	 * @return true if pipeline data was successfully recovered
	 * @throws IOException if status file could not be updated
	 */
	public boolean recover(long timestamp, TroilkattStatus status) throws IOException {
		logger.fatal("\nRecover pipeline " + name + " at " + timestamp);
	
		try {					
			ArrayList<String> inputFiles = null;			
			String lastStatus = status.getStatus(source.stageName, timestamp);
			
			// Retrive files to re-process						
			status.setStatus(source.stageName, timestamp, "start");
			if ((lastStatus != null) && (lastStatus.equals("done"))) {
				// Last iteration succeeded
				logger.info("Recover source");
				inputFiles = source.recover(timestamp);
				status.setStatus(source.stageName, timestamp, "recovered");
			}
			else {
				// Last iteration failed, so it is re-run
				logger.info("Cannot recover source since it was not run");
				inputFiles = source.retrieve2(timestamp);
			}
			status.setStatus(source.stageName, timestamp, "done");			
			logger.info("Retrieved during recovery: " + inputFiles.size());
		
			// Process data        						}
			for (Stage s: pipeline) {
				if ((s == source) || (s == sink)) {
					continue;
				}				
				lastStatus = status.getStatus(s.stageName, timestamp);
				
				status.setStatus(s.stageName, timestamp, "start");
				if ((lastStatus != null) && (lastStatus.equals("done"))) {
					// Last iteration succeeded
					logger.info("Recover stage " + s.stageName);
					inputFiles = s.recover(inputFiles, timestamp);
				}
				else {
					// Last iteration failed, so it is re-run
					logger.info("Cannot recover stage since it was not run: " + s.stageName);
					inputFiles = s.process2(inputFiles, timestamp);
				}
				status.setStatus(s.stageName, timestamp, "done");				
				logger.info("Recovered: " + inputFiles.size());

			}
			
			// Execute sink
			// Since the sink is the last stage it can never have completed, since
			// otherwise the pipeline would not have failed
			status.setStatus(sink.stageName, timestamp, "start");
			sink.sink2(inputFiles, timestamp);
			status.setStatus(sink.stageName, timestamp, "done");
			logger.info("Sunk during recovery: " + inputFiles.size());
			return true;
		} catch (StageException e) {
			logger.error("Could not process a pipeline stage", e);					
			return false;
		}
	}

	/**
	 * Cleanup the output directories by deleting files that are older than 
	 * storageTime. Also delete all temporary files created on the local fs
	 * 
	 * @param timestamp current timestamp
	 * @return list of cleaned directories 
	 */
	public ArrayList<String> cleanup(long timestamp) {
		ArrayList<String> cleanedDirs = new ArrayList<String>();
		
		for (Stage s: pipeline) {
			if (s.hdfsOutputDir == null) { // stage does not save any output files
				continue;
			}
			
			logger.info("Cleanup " + s.hdfsOutputDir);
			// Cleanup HDFS output files
			try {
				tfs.cleanupDir(s.hdfsOutputDir, timestamp, s.storageTime);
				cleanedDirs.add(s.hdfsOutputDir);
			} catch (IOException e) {
				logger.error("IOException during cleanup of " + s.getStageID(), e);
			}			
			
			logger.info("Cleanup " + s.hdfsMetaDir);
			// Cleanup HDFS output files
			try {
				tfs.cleanupMetaDir(s.hdfsMetaDir, timestamp, s.storageTime);
				cleanedDirs.add(s.hdfsMetaDir);
			} catch (IOException e) {
				logger.error("IOException during cleanup of metadir " + s.getStageID(), e);
			}
		}		
		
		
		return cleanedDirs;
	}


	/**
	 * Open a single dataset.
	 *
	 * @param pipelineFile dataset XML file.
	 * @param troilkattProperties troilkatt properties dictionary.
	 * @param hdfsConf hadoop configuration object
	 * @param hdfs HDFS handle
	 * @param logger callers logger object
	 * @return Pipeline object
	 * @throws PipelineException if configuration file cannot be parsed
	 * @throws IOException 
	 * @throws TroilkattPropertiesException 
	 */
	public static Pipeline openPipeline(String pipelineFile, 
			TroilkattProperties troilkattProperties,  
			TroilkattFS tfs,
			Logger logger) throws PipelineException, TroilkattPropertiesException {
	
		if (pipelineFile.indexOf(".xml") == -1) {
			logger.fatal("All pipelines should be named using their .xml file: " + pipelineFile);
			throw new PipelineException("Pipeline name error");
		}
	
		String name = OsPath.basename(pipelineFile).split("\\.")[0];
		logger.info("Open dataset: " + name); 
		
		Pipeline p = null;
		try {
			p = new Pipeline(name,    
					pipelineFile,  
					troilkattProperties,				
					tfs);
		} catch (StageInitException e) {
			logger.fatal("Could not create a stage in pipeline");
			throw new PipelineException("Could not create a stage in pipeline");
		}
		return p;
	}


	/**
	 * Create all pipelines specified in dataset file
	 *
	 * @param datasetFile: file specifying dataset names to be crawled
	 * @param troilkattProperties: troilkatt properties object
	 * @param hdfsConf hadoop configuration object
	 * @param hdfs HDFS handle
	 * @param logger: callers logger instance
	 *  
	 * @return list of pipelines
	 * @throws PipelineException if a pipeline configuration file could not be parsed
	 * @throws TroilkattPropertiesException 
	 */
	public static ArrayList<Pipeline> openPipelines(String datasetFile, 
			TroilkattProperties troilkattProperties,  
			TroilkattFS tfs,
			Logger logger) throws PipelineException, TroilkattPropertiesException {
	
		ArrayList<Pipeline> datasets = new ArrayList<Pipeline>();
		BufferedReader inputStream;
	
		try {
			inputStream = new BufferedReader(new FileReader(datasetFile));
	
			while (true) {
				String pipelineFile = inputStream.readLine();
				if (pipelineFile == null) {
					break;
				}
	
				if (pipelineFile.startsWith("#")) {
					continue;
				}
				
				pipelineFile = pipelineFile.trim();
				if (pipelineFile.indexOf(".xml") == -1) {
					logger.fatal("All pipelines should be named using their .xml file: " + pipelineFile);
					throw new PipelineException("Pipeline name error: " + pipelineFile);
				}
	
				String name;
				try {
					name = OsPath.basename(pipelineFile).split("\\.")[0];
				} catch (ArrayIndexOutOfBoundsException e) {
					logger.fatal("Could not parse dataset name: " + pipelineFile);
					throw new PipelineException("Pipeline name error: " + pipelineFile);
				}
				logger.info("Create pipeline: " + name);
					
				Pipeline p;
				try {
					p = new Pipeline(name,
							pipelineFile,
							troilkattProperties,
							tfs);
					datasets.add(p);
				} catch (StageInitException e) {
					logger.fatal("Could not create a stage in pipeline: " + e.getMessage());
					throw new PipelineException("Could not create a stage in pipeline");
				}
					
			}			
			inputStream.close();
		} catch (IOException e) {
			logger.fatal("Could not parse dataset file: " + datasetFile);
			logger.fatal(e.toString());
			throw new PipelineException("Could not parse dataset file");
		}
	
		return datasets;
	}


	/**
	 * Parse pipeline file and initialize pipeline.
	 * 
	 * @param datasetFile filename of XML file with the pipeline specification.
	 * @throws PipelineException if the pipeline specification could not be parsed
	 * @throws IOException
	 * @throws TroilkattPropertiesException 
	 * @throws StageInitException 
	 */
	private void createPipeline(String datasetFile) throws PipelineException, TroilkattPropertiesException, StageInitException {
		logger.info("Parse pipeline configuration file:" + datasetFile);

		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();		
		DocumentBuilder builder;
		try {
			builder = dbf.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			logger.fatal("Failed to create XML document builder: " + e.toString());
			throw new PipelineException("XML initialization failed");
		}		
		Document xmldoc = null;
		try {
			try {
				xmldoc = builder.parse(datasetFile);
			} catch (IOException e) {
				logger.fatal("Could not parse dataset file: " + e);
				throw new PipelineException("Could not parse dataset file: " + datasetFile);
			}
		} catch (SAXException e) {
			logger.fatal("Configuration file parsing failed for file: " + datasetFile);
			logger.fatal(e.toString());
			throw new PipelineException("Configuration file parsing failed");
		} 

		/*
		 * Get and initialize source
		 */
		NodeList sourceList = xmldoc.getElementsByTagName("source");
		if (sourceList.getLength() > 1) {
			logger.fatal("More than one source specified in:" + datasetFile);
			throw new PipelineException("Error parsing: " + datasetFile);
		}
		else if (sourceList.getLength() < 1) {
			logger.fatal("No source specified in pipeline configuration:" + datasetFile);
			throw new PipelineException("Error parsing: " + datasetFile);
		}
		Element sourceElement = (Element) sourceList.item(0);
		source = createSource(sourceElement);   	    

		/*
		 * Get and initialize pipeline stages
		 */
		NodeList stageList = xmldoc.getElementsByTagName("stage");
		int stageNum = 1;   		
		for (int i = 0; i < stageList.getLength(); i++) {
			Element s = (Element) stageList.item(i);			
			Stage newStage = createStage(s, stageNum);			
			pipeline.add(newStage);            
			stageNum += 1;			
		}

		/*
		 * Get and initialize sink
		 */
		NodeList sinkList = xmldoc.getElementsByTagName("sink");
		if (sinkList.getLength() > 1) {
			logger.fatal("More than one sink specified in: " +  datasetFile);
			throw new PipelineException("Error parsing " + datasetFile);
		}
		else if (sinkList.getLength() < 1) {
			logger.fatal("Sink not specified in: " +  datasetFile);
			throw new PipelineException("Error parsing " + datasetFile);
		}
		Element sinkElement = (Element) sinkList.item(0);
		sink = createSink(sinkElement, stageNum);		
	}
	
	/**
	 * Helper function to create a Source object
	 * 
	 * @param s source XML element
	 * @return initialized Source object of the type specified in the configuration
	 * file.
	 * @throws PipelineException  if XML element cannot be parsed
	 * @throws StageInitException 
	 * @throws TroilkattPropertiesException 
	 */
	private Source createSource(Element s) throws PipelineException, TroilkattPropertiesException, StageInitException {
		String nameText =  parseElementText(s, "name");
		String typeText =  parseElementText(s, "type");
		String argsText =  parseElementText(s, "arguments");
		String outputDir =  parseElementText(s, "output-directory");
		String compressionFormat = parseElementText(s, "compression-format");
		int storageTime = -1;
		if (outputDir.isEmpty()) {
			logger.info("No output directory for source: " + nameText);
		}
		else  {
			try {
				storageTime = Integer.valueOf(parseElementText(s, "storage-time"));
			} catch (NumberFormatException e) {
				throw new PipelineException("Invalid storage time for source: " + nameText);
			}
		}
		
		logger.debug("Adding source: " + nameText);            				
		
		return SourceFactory.newSource(typeText,				
				nameText, 
				argsText, 
				outputDir,
				compressionFormat,
				storageTime,		
				this,
				logger);
	}

	/**
	 * Helper function to create a Stage object
	 * 
	 * @param s stage XML element
	 * @param stageNum stage number in pipeline
	 * @return initialized Stage object of the type specified in the configuration
	 * file.
	 * @throws PipelineException if XML element cannot be parsed
	 * @throws TroilkattPropertiesException 
	 * @throws StageInitException 
	 */
	private Stage createStage(Element s, int stageNum) throws PipelineException, TroilkattPropertiesException, StageInitException {
		String nameText =  parseElementText(s, "name");
		String typeText =  parseElementText(s, "type");
		String argsText =  parseElementText(s, "arguments");
		String outputDir =  parseElementText(s, "output-directory");
		String compressionFormat = parseElementText(s, "compression-format");
		int storageTime = -1;
		if (outputDir.isEmpty()) {
			logger.info("No output directory for stage: " + nameText);
		}
		else  {					
			try {
				storageTime = Integer.valueOf(parseElementText(s, "storage-time"));
			} catch (NumberFormatException e) {
				throw new PipelineException("Invalid storage time for stage: " + nameText);
			}
		}	

		logger.debug("Adding stage: " + nameText);            
		return StageFactory.newStage(typeText,
				stageNum,
				nameText, 
				argsText, 
				outputDir,
				compressionFormat,
				storageTime,
				this,				
				logger);
	}

	/**
	 * Helper function to create a Sink object
	 * 
	 * @param s sink XML element
	 * @param stageNum stage number in pipeline
	 * @return initialized Sink object of the type specified in the configuration
	 * file.
	 * @throws StageInitException 
	 * @throws TroilkattPropertiesException 
	 * @throws PipelineException 
	 * @throws PipelineException if XML element cannot be parsed
	 */
	private Sink createSink(Element sinkElement, int stageNum) throws TroilkattPropertiesException, StageInitException, PipelineException {
		String nameText = parseElementText(sinkElement, "name");
		String typeText = parseElementText(sinkElement, "type");
		String argsText = parseElementText(sinkElement, "arguments");
		
		logger.debug("Adding sink: " + nameText); 
		return SinkFactory.newSink(typeText,
				stageNum,
				nameText, 
				argsText,	
				this,										
				logger);
	}

	/**
	 * Parse configuration file helper function used to get text of a node.
	 *
	 * @param node minidom node that contains only one text tag
	 * @param tagName tag to search for in tree
	 * @param required set to true; if the element is required and hence must be specified. If
	 *   set an exception is raised id the element is not found. Otherwise an empty list
	 *   is returned for not found elements.
	 *  
	 * @return stripped text or null if tag is not found. 
	 * @throws PipelineException 
	 */
	private String parseElementText(Element node, String tagName, boolean required) throws PipelineException {
		NodeList list = node.getElementsByTagName(tagName);
	
		if (required) {
			if (list.getLength() < 1) {
				logger.fatal("Tag not found: " + tagName);
				logger.fatal(node.toString());
				throw new PipelineException("Configuration file parse error: tag" + tagName + " not found");
			}
			else if (list.getLength() > 1) {
				logger.fatal("Multiple nodes for tag: %s: " + tagName);
				logger.fatal(node.toString());
				throw new PipelineException("Configuration file parse error");
			}
		}
		else {
			if (list.getLength() == 0) {
				return "";
			}
		}        
	
		if (list.item(0).getFirstChild() == null) {
			return "";
		}
	
		return list.item(0).getFirstChild().getTextContent();
	}

	/**
	 * Parse configuration file helper function used to get text of a node.
	 *
	 * @param node minidom node that contains only one text tag
	 * @param tagName tag to search for in tree
	 * @return stripped text or null if tag is not found. 
	 * @throws PipelineException 
	 */
	private String parseElementText(Element node, String tagName) throws PipelineException {
		return parseElementText(node, tagName, true);
	}
}

