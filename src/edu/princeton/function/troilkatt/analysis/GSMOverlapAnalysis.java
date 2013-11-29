package edu.princeton.function.troilkatt.analysis;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import edu.princeton.function.troilkatt.fs.OsPath;
import edu.princeton.function.troilkatt.tools.GeoGSMOverlap;

/**
 * Analyze GSM sample overlap
 */
public class GSMOverlapAnalysis {
	/**
	 * Placeholder for data read from overlap file
	 */
	public class Overlap {		
		public String gid1;
		public String gid2;		
		// Line read from overlap file
		public String line;
		
		public Overlap(String g1, String g2, String l) {
			gid1 = g1;
			gid2 = g2;
			line = l;
		}
	}
	
	public HashMap<String, ArrayList<String>> gse2gsm;	
	public HashMap<String, ArrayList<String>> gsm2gse;	
	public HashMap<String, Date> gse2date;
	public ArrayList<Overlap> overlap;
	
	public GSMOverlapAnalysis() {
		gse2gsm = new HashMap<String, ArrayList<String>>();
		gsm2gse = new HashMap<String, ArrayList<String>>();
		gse2date = new HashMap<String, Date>();
		overlap = new ArrayList<Overlap>();
	}
	
	/**
	 * Create data structure for gse2gsm and the reverse gsm2gse mappings.
	 * 
	 * @param filename gseTable filename
	 * @throws ParseException 
	 * @throws IOException 
	 */
	public void parseGSETable(String filename) throws IOException, ParseException {
		BufferedReader ins = new BufferedReader(new FileReader(filename));
		SimpleDateFormat dateFormatter = new SimpleDateFormat("MMM dd yyyy");
		
		// skip header
		ins.readLine();
		
		String line;
		while ((line = ins.readLine()) != null) {
			String[] parts = line.split("\t");
			if (parts.length < 5) {
				System.err.println("Ignoring line: " + line);
				continue;
			}
			String gid = parts[0];				

			Date date = dateFormatter.parse(parts[1]);
			if (gse2date.containsKey(gid)) {
				ins.close();
				throw new RuntimeException("Invalid table file: multiple entries for: " + gid);
			}
			gse2date.put(gid, date);

			//String[] orgs = parts[2].split(",");
			//String gpl = parts[3];

			ArrayList<String> newGsms = new ArrayList<String>();
			String[] gsms = parts[4].split(","); 
			for (String gsm: gsms) {
				newGsms.add(gsm);

				ArrayList<String> m2e = gsm2gse.get(gsm);
				if (m2e == null) {
					m2e = new ArrayList<String>();
					gsm2gse.put(gsm, m2e);
				}
				if (m2e.contains(gid)) {
					ins.close();
					throw new RuntimeException("Invalid table file: multiple entries for: " + gsm);
				}
				m2e.add(gid);					
			}
			if (gse2gsm.containsKey(gid)) {
				ins.close();
				throw new RuntimeException("Invalid table file: multiple entries for: " + gid);
			}
			gse2gsm.put(gid, newGsms);
		} 

		ins.close();
	}
	
	/**
	 * Create data structure for the overlap file content
	 * 
	 * @param overlap filename
	 */
	public void parseOverlapFile(String filename) throws IOException {
		BufferedReader ins = new BufferedReader(new FileReader(filename));
		
		HashSet<String> added = new HashSet<String>();
		
		String line;
		while ((line = ins.readLine()) != null) {
			String[] parts = line.split("\t");
			String gid1 = parts[0];
			String gid2 = parts[1];
			
			if (GeoGSMOverlap.compareIDs(gid1, gid2) < 0) {
				String tuple = gid1 + "," + gid2;
				if (added.contains(tuple)) {
					ins.close();
					throw new RuntimeException("Duplicate: (" + tuple + ")");
				}
				added.add(tuple);
			}
			
			//if (FindGSMOverlap.compareIDs(gid1, gid2) < 0) {
			Overlap n = new Overlap(gid1, gid2, line);
			overlap.add(n);
			//}
		}
		
		ins.close();
	}
	
	/**
	 * Find and reduce overlap for each month in the period
	 * 
	 * @param startDate (YEAR.MONTH, e.g. 2011.01)
	 * @param endDate (YEAR.MONTH, e.g. 2011.12)
	 * @throws ParseException 
	 * @throws edu.princeton.function.troilkatt.tools.ParseException 
	 * @throws IOException 
	 */
	public void doAnalysis(String startDate, String endDate, String outputDir) throws ParseException, edu.princeton.function.troilkatt.tools.ParseException, IOException {
		SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy.MM");
		Date start = dateFormatter.parse(startDate);
		Date end = dateFormatter.parse(endDate);
		
		GregorianCalendar cal = new GregorianCalendar();
		cal.setTime(start);
		
		GeoGSMOverlap parser = new GeoGSMOverlap();
		
		/*
		 * For each month, find all GSEs published before that date and run analysis
		 */
		System.out.println("Analyze for period: " + startDate + " to " + endDate);
		while (true) {
			Date cur = cal.getTime();
			if (cur.after(end)) {
				break;
			}
			String curDir = OsPath.join(outputDir, cal.get(Calendar.YEAR) + "." + cal.get(Calendar.MONTH));
			OsPath.mkdir(curDir);
			System.out.println("In month " + cal.get(Calendar.MONTH) + " in year " + cal.get(Calendar.YEAR));
			
			// Parse 
			HashSet<String> gses = filterGSEs(cur);
			for (Overlap o: overlap) {
				if (gses.contains(o.gid1) && gses.contains(o.gid2)) {
					parser.addOverlapLine(o.line);
				}
			}
			// Print stats 
			parser.findClusters();
			ArrayList<ArrayList<String>> clusters = parser.getClusterIDs(2);
			writeClusters(OsPath.join(curDir, "clusters.2.before"), clusters);
			
			// Find overlap
			parser.find(3, 3);
			
			// Print stats2						
			writeGse2Gsm(OsPath.join(curDir, "gse2gsm.before"), gses);
			writeGsm2Gse(OsPath.join(curDir, "gsm2gse.before"), gses);
			writeGids(OsPath.join(curDir, "gseList"), gses);
			
			HashSet<String> allRemoved = new HashSet<String>(); 			
			HashSet<String> duplicates = parser.getDuplicateIDs();
			if (hasDuplicate(duplicates)) {
				throw new RuntimeException("Duplicates has duplicate GIDs");
			}
			writeGids(OsPath.join(curDir, "duplicates"), duplicates);
			allRemoved.addAll(duplicates);
			
			HashSet<String> supersets = parser.getSupersetIDs();
			if (hasOverlap(supersets, allRemoved)) {
				throw new RuntimeException("Supersets and duplicates overlap");
			}
			if (hasDuplicate(supersets)) {
				throw new RuntimeException("Supersets has duplicate");
			}
			writeGids(OsPath.join(curDir, "supersets"), supersets);
			allRemoved.addAll(supersets);
			
			HashSet<String> removed3 =  parser.getRemovedIDs();
			if (hasOverlap(removed3, allRemoved)) {
				throw new RuntimeException("removed.3 overlap with either supersets or duplicates overlap");
			}
			if (hasDuplicate(removed3)) {
				throw new RuntimeException("Supersets has duplicate");
			}
			writeGids(OsPath.join(curDir, "removed.3"), removed3);
			allRemoved.addAll(removed3);
			
			HashMap<String, HashSet<String>> removedSamples = parser.getRemovedSamples();	
			Set<String> removedSampleKeys = removedSamples.keySet();
			ArrayList<String> removedSampleKeysList = new ArrayList<String>(removedSampleKeys);
			
			writeRemovedSamples(OsPath.join(curDir, "removedSamples"), removedSamples);
			if (hasOverlap(removedSampleKeysList, allRemoved)) {
				throw new RuntimeException("removedSample key overlap with removed sample key");
			}
			writeGse2Gsm(OsPath.join(curDir, "gse2gsm.after"), gses, allRemoved, removedSamples);
			writeGsm2Gse(OsPath.join(curDir, "gsm2gse.after"), gses, allRemoved, removedSamples);
			
			clusters = parser.getClusterIDs(2);
			writeClusters(OsPath.join(curDir, "clusters.2.after"), clusters);
			
			parser.reset();
			cal.add(Calendar.MONTH, 1);
		}
		
		System.out.println("Done");
	}
	
	/**
	 * Check if the a list and a set have any overlap
	 * 
	 * @param l1 first list
	 * @param l2 second list
	 * @return true if there is overlap
	 */
	private boolean hasOverlap(ArrayList<String> l1, HashSet<String> s2) {
		boolean rv = false;
		
		for (String s: l1) {
			if (s2.contains(s)) {
				System.err.println("Found overlapping item: " + s);
				rv = true;
			}
		}
		
		System.out.printf("Checked %d against %d items for overlap\n", l1.size(), s2.size());
		
		return rv;
	}
	
	/**
	 * Check if the a list and a set have any overlap
	 * 
	 * @param l1 first list
	 * @param l2 second list
	 * @return true if there is overlap
	 */
	private boolean hasOverlap(HashSet<String> l1, HashSet<String> s2) {
		boolean rv = false;
		
		for (String s: l1) {
			if (s2.contains(s)) {
				System.err.println("Found overlapping item: " + s);
				rv = true;
			}
		}
		
		System.out.printf("Checked %d against %d items for overlap\n", l1.size(), s2.size());
		
		return rv;
	}
	
	/**
	 * Check if a list has duplicates
	 * 
	 * @param l list to check
	 */
	private boolean hasDuplicate(HashSet<String> l1) {
		boolean rv = false;
		
		HashSet<String> map = new HashSet<String>(); 
		
		for (String s: l1) {
			if (map.contains(s)) {
				System.err.println("Found duplicate: " + s);
				rv = true;
			}
			map.add(s);
		}
		
		return rv;
	}
	
	/**
	 * Get a set of series published before the provided date
	 * 
	 * @param cur date that series have to be published before
	 * @return
	 */
	private HashSet<String> filterGSEs(Date cur) {
		HashSet<String> gses = new HashSet<String>();
		
		for (String g: gse2date.keySet()) {
			if (cur.after(gse2date.get(g))) {
				gses.add(g);
			}
		}
		
		return gses;
	}

	/**
	 * Write the GSE-to-GSM ID mappings for the GSE IDs specified in the gses parameter
	 * 
	 * File format, for each set there is one line
	 * GID<tab>GSM1<tab>GSM2<tab>GSM3>....GSEN<newline>
	 * 
	 * @param filename output filename
	 * @param gses list of GSE IDs that are to be written to the output file
	 * @return none 
	 * @throws IOException 
	 */
	private void writeGse2Gsm(String filename, HashSet<String> gses) throws IOException {
		BufferedWriter os = new BufferedWriter(new FileWriter(filename));
		
		for (String gid: gses) {
			ArrayList<String> gsms = gse2gsm.get(gid);
			if (gsms == null) {
				os.close();
				throw new RuntimeException("GID not found: " + gid);
			}
			os.write(gid);
			for (String gsm: gsms) {
				os.write("\t" + gsm);
			}
			os.write("\n");
		}
		
		os.close();
	}

	/**
	 * Write the GSE-to-GSM ID mappings for the GSE IDs specified in the gses list, but
	 * excluding sampels specified in the removedSamples map
	 * 
	 * File format, for each set there is one line
	 * GID<tab>GSM1<tab>GSM2<tab>GSM3>....GSEN<newline>
	 * 
	 * @param filename output filename
	 * @param gses list of GSE IDs that are to be written to the output file
	 * @param removedGids IDs of sets removed due to being a duplicate, superset, or having too few samples
	 * @param removedSamples map of (GID->[samples]) of samples that should be excluded
	 * @return none 
	 * @throws IOException 
	 */
	private void writeGse2Gsm(String filename, HashSet<String> gses,
			HashSet<String> removedGids, HashMap<String, HashSet<String>> removedSamples) throws IOException {
		BufferedWriter os = new BufferedWriter(new FileWriter(filename));
		
		for (String gid: gses) {
			if (removedGids.contains(gid)) { // has been removoed
				continue;
			}
			
			ArrayList<String> gsms = gse2gsm.get(gid);
			if (gsms == null) {
				os.close();
				throw new RuntimeException("GID not found: " + gid);
			}
			HashSet<String> excluded = removedSamples.get(gid);
			
			String samplesLine = "";
			for (String s: gsms) {
				if (excluded != null) {
					if (excluded.contains(s) == false) {
						samplesLine = samplesLine + "\t" + s;
					}
					// else: sample was removed and is therefore not written
				}
				else { // no samples are excluded for this set
					samplesLine = samplesLine + "\t" + s;
				}
			}
			
			if (! samplesLine.isEmpty()) {
				os.write(gid + samplesLine + "\n");
			}
		}
		
		os.close();		
	}

	/**
	 * Write the GSM-to-GSE ID mappings for the GSE IDs specified in the gses parameter
	 * 
	 * File format, for each sample there is one line:
	 * GSM<tab>GSE1<tab>GSE2<tab>GSE3>...GSEN<newline>
	 * 
	 * @param filename output filename
	 * @param gses list of GSE IDs that are to be written to the output file
	 * @return none 
	 * @throws IOException 
	 */
	private void writeGsm2Gse(String filename, HashSet<String> gses) throws IOException {
		BufferedWriter os = new BufferedWriter(new FileWriter(filename));
		
		for (String gsm: gsm2gse.keySet()) {
			ArrayList<String> gsms = gsm2gse.get(gsm);			
			if (gsms == null) {
				os.close();
				throw new RuntimeException("GSM ID not found: " + gsm);
			}
			String gsmLine = "";
			for (String gid: gsm2gse.get(gsm)) {
				if (gses.contains(gid)) {
					gsmLine = gsmLine + "\t" + gid;
				}
			}
			
			if (! gsmLine.isEmpty()) {
				os.write(gsm + gsmLine + "\n");
			}			
		}
		
		os.close();
	}

	/**
	 * Write the GSM-to-GSE ID mappings for the GSE IDs specified in the gses list, but
	 * excluding sampels specified in the removedSamples map
	 * 
	 * File format, for each sample there is one line:
	 * GSM<tab>GSE1<tab>GSE2<tab>GSE3>...GSEN<newline>
	 * 
	 * @param filename output filename
	 * @param gses list of GSE IDs that are to be written to the output file
	 * @param removedGids IDs of sets removed due to being a duplicate, superset, or having too few samples
	 * @param removedSamples map of (GID->[samples]) of samples that should be excluded
	 * @return none 
	 * @throws IOException 
	 */
	private void writeGsm2Gse(String filename, HashSet<String> gses,
			HashSet<String> removedGids, HashMap<String, HashSet<String>> removedSamples) throws IOException {
		BufferedWriter os = new BufferedWriter(new FileWriter(filename));
		
		for (String gsm: gsm2gse.keySet()) {
			ArrayList<String> gsms = gsm2gse.get(gsm);			
			if (gsms == null) {
				os.close();
				throw new RuntimeException("GSM ID not found: " + gsm);
			}
			String gsmLine = "";
			for (String gid: gsm2gse.get(gsm)) {				
				if (gses.contains(gid) == false) { // not yet published
					continue;
				}
				if (removedGids.contains(gid)) { // has been removed
					continue;
				}
				
				HashSet<String> excluded = removedSamples.get(gid);
				if (excluded != null) {
					if (excluded.contains(gsm) == false) {
						gsmLine = gsmLine + "\t" + gid;
					}
					// else sample excluded form this dataset
				}
				else { // No samples excluded from set
					gsmLine = gsmLine + "\t" + gid;
				}
			}
			
			if (! gsmLine.isEmpty()) {
				os.write(gsm + gsmLine + "\n");
			}			
		}
		
		os.close();
		
	}

	/**
	 * Write the list of GIDS to an output file
	 * 
	 * File format, for each GID there is one line
	 * <GID><newline>
	 * 
	 * @param filename output filename
	 * @param gids IDs to write to output file
	 * @throws IOException 
	 */
	//private void writeGids(String filename, ArrayList<String> gids) throws IOException {
	//	BufferedWriter os = new BufferedWriter(new FileWriter(filename));
	//	
	//	for (String g: gids) {
	//		os.write(g + "\n");			
	//	}
	//	
	//	os.close();		
	//}

	/**
	 * Write the set of GIDS to an output file
	 * 
	 * File format, for each GID there is one line
	 * <GID><newline>
	 * 
	 * @param filename output filename
	 * @param gids IDs to write to output file
	 * @throws IOException 
	 * @throws IOException 
	 */
	private void writeGids(String filename, HashSet<String> gids) throws IOException {
		BufferedWriter os = new BufferedWriter(new FileWriter(filename));
		
		for (String g: gids) {
			os.write(g + "\n");			
		}
		
		os.close();
	}

	/**
	 * Write removed samples to an output fil
	 * 
	 * File format, for each set with removed samples:
	 * <GID><tab>GSM1<tab>GSM2...GSMn<newline>
	 * 
	 * @param filename output filename
	 * @param removedSamples removed samples
	 * @throws IOException 
	 */
	private void writeRemovedSamples(String filename, HashMap<String, HashSet<String>> removedSamples) throws IOException {		
		BufferedWriter os = new BufferedWriter(new FileWriter(filename));
		
		for (String g: removedSamples.keySet()) {
			os.write(g);
			HashSet<String> gsms = removedSamples.get(g);
			for (String s: gsms) {
				os.write("\t" + s);
			}
			os.write("\n");
		}
		
		os.close();
	}

	/**
	 * Write clusters to an output file 
	 * 
	 * File format, for each cluster threre is one line:
	 * GID1<tab>GID2<tab>...<GIDN><newline>
	 * 
	 * @param filename output filename
	 * @param clusters clusters to write
	 * @throws IOException 
	 */
	private void writeClusters(String filename,	ArrayList<ArrayList<String>> clusters) throws IOException {
		BufferedWriter os = new BufferedWriter(new FileWriter(filename));
		
		for (ArrayList<String> cl: clusters) {
			os.write(cl.get(0));
			for (int i = 1; i < cl.size(); i++) {
				os.write(cl.get(i));
			}
			os.write("\n");			
		}
		
		os.close();		
	}
	

	/**
	 * @param args command line arguments
	 *  0: gseTable.csv file that contains meta information and samples for each GEO series
	 *  1: gse.overlap file
	 *  2: output directory
	 *  3: start date (YEAR.MONTH, e.g. 2011.01)
	 *  4: end data (YEAR.MONTH, e.g. 2011.12)
	 * @throws ParseException 
	 * @throws IOException 
	 * @throws edu.princeton.function.troilkatt.tools.ParseException 
	 */
	public static void main(String[] args) throws IOException, ParseException, edu.princeton.function.troilkatt.tools.ParseException {
		if (args.length != 5) {
			System.err.println("Usage: gseTable.csv gse.overlap outputDir startDate endDate");
			System.exit(-1);
		}

		GSMOverlapAnalysis an = new GSMOverlapAnalysis();
		an.parseGSETable(args[0]);
		an.parseOverlapFile(args[1]);
		an.doAnalysis(args[3], args[4], args[2]);
	}

}
