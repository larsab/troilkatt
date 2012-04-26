

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/*
 * Created on Jun 9, 2005
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */

/**
 * @author mhibbs
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class MedianMultiples {

	public class Gene  {
		public String orf;
		public String name;
	  public float[] exprLvls;

	  public Gene (String _orf, String _name) {
	    	orf = _orf;
	    	name = _name;
	  }
	}
	
	public HashMap geneSets;
	public HashMap genes;
	public HashMap orf2gene;
	public ArrayList exprNames;
	int numExprs;

	public MedianMultiples() {
		geneSets = new HashMap();
		genes = new HashMap();
		orf2gene = new HashMap();
		exprNames = new ArrayList();
		numExprs = 0;
	}

	/** Clears out all data, and reads in new data in a pcl file from the
	 *  buffered reader provided
	 */
	public boolean readDataFromPCL(BufferedReader fin) throws IOException {
		// Clear out all arrays and re-initialize
		geneSets.clear();
		numExprs = 0;

		// Try to read in the data from the buffered reader
		try {
			int row = 0;
			int gweightCol = -1;
			int colOffset = 2;

			//While there's more data to read in...
			String line = fin.readLine();
			while ((line != null) && (!line.equals(""))) {
				boolean processRow = true;

				//If this is the first row, this determines number of experiments
				if (row == 0) {
					String[] names = line.split("\t");
					for (int i = 2; i < names.length; i++) {
						//If there is a GWEIGHT column, make a note
						if (names[i].equalsIgnoreCase("GWEIGHT")) {
							gweightCol = i;
							colOffset = 3;
						}
						//Otherwise, record the experiment name
						else {
							exprNames.add(names[i]);
							numExprs++;
						}
					}
					if (names.length < 4) {
						return false;
					}
					processRow = false;
				}

				//If this is the second row, it may contain weights, or it may
				//contain data
				else if (row == 1) {
					String[] lineParts = line.split("\t");

					//If the first column is EWEIGHT, then just ignore this row
					if (lineParts[0].equalsIgnoreCase("EWEIGHT")) {
						processRow = false;
					}
				}

				//If the line is up for processing...
				if (processRow) {
					String[] lineParts = line.split("\t");

					if (lineParts.length > 1) {
						//Columns 0 and 1 are the SpotIDs and Names
						Set geneSet;
						Object obj = geneSets.get(lineParts[1].toUpperCase());
						if (obj != null) {
							geneSet = (Set) obj;
						}
						else {
							geneSet = new HashSet();
							geneSets.put(lineParts[1].toUpperCase(), geneSet);
						}
						Gene gene = new Gene(lineParts[0].toUpperCase(), lineParts[1].toUpperCase());
						gene.exprLvls = new float[numExprs];
						for (int e = 0; e < numExprs; e++) {
							gene.exprLvls[e] = 0;
						}
						geneSet.add(gene);
						
						obj = orf2gene.get(lineParts[0].toUpperCase());
						if (obj == null) {
							orf2gene.put(lineParts[0].toUpperCase(), gene);
						}
						

						//Columns from 2 to the end are data points (except the
						// gweightCol)
						for (int i = 2; i < lineParts.length; i++) {
							if (i != gweightCol) {
								//Try to convert the string to a number, if you
								// fail, just fill in with 0s
								try {
									float val = Float.parseFloat(lineParts[i]);
									if (!Float.isNaN(val)) {									
										gene.exprLvls[i - colOffset] += val;
									}

								}
								catch (NumberFormatException except) {
								}
							}
						}
					}
				}

				//Increment row number and repeat
				row++;
				line = fin.readLine();
			}}
			catch (IOException except) {
				throw (except);
			}
			
		//Finally, return success
		return true;
	}

public boolean readDataFromPCLleaveCase(BufferedReader fin) throws IOException {
		// Clear out all arrays and re-initialize
		geneSets.clear();
		numExprs = 0;

		// Try to read in the data from the buffered reader
		try {
			int row = 0;
			int gweightCol = -1;
			int colOffset = 2;

			//While there's more data to read in...
			String line = fin.readLine();
			while ((line != null) && (!line.equals(""))) {
				boolean processRow = true;

				//If this is the first row, this determines number of experiments
				if (row == 0) {
					String[] names = line.split("\t");
					for (int i = 2; i < names.length; i++) {
						//If there is a GWEIGHT column, make a note
						if (names[i].equalsIgnoreCase("GWEIGHT")) {
							gweightCol = i;
							colOffset = 3;
						}
						//Otherwise, record the experiment name
						else {
							exprNames.add(names[i]);
							numExprs++;
						}
					}
					if (names.length < 4) {
						return false;
					}
					processRow = false;
				}

				//If this is the second row, it may contain weights, or it may
				//contain data
				else if (row == 1) {
					String[] lineParts = line.split("\t");

					//If the first column is EWEIGHT, then just ignore this row
					if (lineParts[0].equalsIgnoreCase("EWEIGHT")) {
						processRow = false;
					}
				}

				//If the line is up for processing...
				if (processRow) {
					String[] lineParts = line.split("\t");

					if (lineParts.length > 1) {
						//Columns 0 and 1 are the SpotIDs and Names
						Set geneSet;
						Object obj = geneSets.get(lineParts[1]);
						if (obj != null) {
							geneSet = (Set) obj;
						}
						else {
							geneSet = new HashSet();
							geneSets.put(lineParts[1], geneSet);
						}
						Gene gene = new Gene(lineParts[0], lineParts[1]);
						gene.exprLvls = new float[numExprs];
						for (int e = 0; e < numExprs; e++) {
							gene.exprLvls[e] = 0;
						}
						geneSet.add(gene);
						
						obj = orf2gene.get(lineParts[0]);
						if (obj == null) {
							orf2gene.put(lineParts[0], gene);
						}
						

						//Columns from 2 to the end are data points (except the
						// gweightCol)
						for (int i = 2; i < lineParts.length; i++) {
							if (i != gweightCol) {
								//Try to convert the string to a number, if you
								// fail, just fill in with 0s
								try {									
									if (!Float.isNaN(val)) {
									if (val != ExpressionDataset.NAN) {
										gene.exprLvls[i - colOffset] += val;
									}

								}
								catch (NumberFormatException except) {
								}
							}
						}
					}
				}

				//Increment row number and repeat
				row++;
				line = fin.readLine();
			}}
			catch (IOException except) {
				throw (except);
			}
			
		//Finally, return success
		return true;
}

	public float euclideanDistance(float[] g1, float[] g2) {
		float result = 0;
		
		for(int i=0; i<g1.length; i++) {
			result += (g1[i] - g2[i]) * (g1[i] - g2[i]);
		}
		result = (float)Math.sqrt(result);
		
		return result;
	}
	
	public float pearsonCorrelation(float[] g1, float[] g2) {
		// Using the formula:
		// Sum(XY) - Sum(X)Sum(Y)/N
		// r = ------------------------------------------------------
		// Sqrt( (Sum(X^2) - Sum(X)^2/N) (Sum(Y^2) - Sum(Y)^2/N)
		float sumx = 0;
		float sumy = 0;
		float sumxx = 0;
		float sumyy = 0;
		float sumxy = 0;
		// Calculate the individual terms
		for (int i = 0; i < g1.length; i++) {
			sumx += g1[i];
			sumy += g2[i];
			sumxx += g1[i] * g1[i];
			sumyy += g2[i] * g2[i];
			sumxy += g1[i] * g2[i];
		}

		// Finish the formula and return
		// return 1 - Math.abs((sumxy - (sumx*sumy)/exprLvls.length) /
		// Math.sqrt((sumxx - (sumx*sumx)/exprLvls.length) * (sumyy -
		// (sumy*sumy)/exprLvls.length)));
		float rho = (sumxy - (sumx * sumy) / g1.length)
				/ (float)Math.sqrt((sumxx - (sumx * sumx) / g1.length)
						* (sumyy - (sumy * sumy) / g1.length));

		if (rho > 1.0) {
			// System.out.println("r=" + Math.abs((sumxy -
			// (sumx*sumy)/exprLvls.length) /
			// Math.sqrt((sumxx - (sumx*sumx)/exprLvls.length) * (sumyy -
			// (sumy*sumy)/exprLvls.length))));
			return 1.0f;
		} else if (rho < -1.0) {
			return -1.0f;
		}
		return rho;
	}
	
	public void makeMedianGene(String key) {
		Set gSet = (Set)geneSets.get(key);
		Gene[] gArray = new Gene [gSet.size()];
		gArray = (Gene[])gSet.toArray(gArray);
		if (gArray.length == 1) {
			genes.put(key,gArray[0]);
		}
		else {
			Gene medGene = new Gene(key, key);
			medGene.exprLvls = new float[numExprs];
			//Find the median of each entry from all the genes in the set
			for(int i=0; i<medGene.exprLvls.length; i++) {
				float[] entries = new float [gArray.length];
				for(int j=0; j<gArray.length; j++) {
					entries[j] = gArray[j].exprLvls[i];
				}
				medGene.exprLvls[i] = median(entries);
			}
			genes.put(key,medGene);
		}
	}
	
	public void makeMeanGeneIfAgree (String key) {
		Set gSet = (Set)geneSets.get(key);
		Gene[] gArray = new Gene [gSet.size()];
		gArray = (Gene[])gSet.toArray(gArray);
		if (gArray.length == 1) {
			genes.put(key,gArray[0]);
		}
		else {
			int[] numAgree = new int [gArray.length];
			for (int i=0; i<gArray.length; i++) {
				for (int j=i+1; j<gArray.length; j++) {
					if (genesMLagree(gArray[i], gArray[j])) {
						numAgree[i] += 1;
						numAgree[j] += 1;
					}
				}
			}
			Gene meanGene = new Gene (key, key);
			meanGene.exprLvls = new float [numExprs];
			int numGood = 0;
			for (int i=0; i<gArray.length; i++) {
				if (numAgree[i] >= ((numAgree.length - 1) / 2.0)) { 
					numGood += 1;
					for (int c=0; c<numExprs; c++) {
						meanGene.exprLvls[c] += gArray[i].exprLvls[c];
					}
				}
			}
			if (numGood > (numAgree.length / 2.0)) {
				for (int c=0; c<numExprs; c++) {
					meanGene.exprLvls[c] /= (float)numGood;
				}
				genes.put(key, meanGene);
				System.err.println(key + " expression averaged from " + numGood + " probes");
			}
			else {
				System.err.println(key + " did not agree, and was discarded");
			}
		}
	}
	
	public float median (float[] array) {
		Arrays.sort(array);
		return array[(int)((float)array.length / 2.0)];
	}
		
	public void meanAllThatAgree() {
		Set keySet = geneSets.keySet();
		String[] keys = new String [keySet.size()];
		keys = (String[]) keySet.toArray(keys);
		for (int i=0; i<keys.length; i++) {
			makeMeanGeneIfAgree(keys[i]);
		}
	}
	
	public void medianAll() {
		Set keySet = geneSets.keySet();
		String[] keys = new String [keySet.size()];
		keys = (String[]) keySet.toArray(keys);
		
		for (int i=0; i<keys.length; i++) {
			makeMedianGene(keys[i]);
		}
	}
	
	public void medianIfAgree(float cutoff) {
		Set keySet = geneSets.keySet();
		String[] keys = new String [keySet.size()];
		keys = (String[]) keySet.toArray(keys);
		
		for (int i=0; i<keys.length; i++) {
			/** Check if this is a pair, and if it disagrees, ignore it **/
			Set gSet = (Set)geneSets.get(keys[i]);
			if (gSet.size() == 2) {
				Gene[] gArray = new Gene [gSet.size()];
				gArray = (Gene[])gSet.toArray(gArray);
				float corr = pearsonCorrelation(gArray[0].exprLvls, gArray[1].exprLvls);
				if (corr > cutoff) {
					makeMedianGene(keys[i]);
				}
			}
			else {
				makeMedianGene(keys[i]);
			}
		}
	}

	public float[] sampleEuclidDists(float rate) {
		Set keys = orf2gene.keySet();
		String[] orfs = new String [keys.size()];
		orfs = (String[])keys.toArray(orfs);
		
		ArrayList dists = new ArrayList();
		for (int i=0; i<orfs.length; i++) {
			for (int j=i+1; j<orfs.length; j++) {
				float r = (float)Math.random();
				if (r < rate) {
					Gene g1 = (Gene)orf2gene.get(orfs[i]);
					Gene g2 = (Gene)orf2gene.get(orfs[j]);
					dists.add(new Float(euclideanDistance(g1.exprLvls, g2.exprLvls)));
				}
			}
		}
		
		float[] result = new float [dists.size()];
		for (int i=0; i<dists.size(); i++) {
			result[i] = ((Float)dists.get(i)).floatValue();
		}
		return result;
	}
	
	public float[] mappedEuclidDists () {
		Set keys = geneSets.keySet();
		String[] names = new String [keys.size()];
		names = (String[]) keys.toArray(names);
		
		ArrayList dists = new ArrayList();
		for (int i=0; i<names.length; i++) {
			Set gSet = (Set)geneSets.get(names[i]);
			if (gSet.size() > 1) {
				Gene[] gArray = new Gene [gSet.size()];
				gArray = (Gene[]) gSet.toArray(gArray);
				for (int x=0; x<gArray.length; x++) {
					for (int y=x+1; y<gArray.length; y++) {
						dists.add(new Float(euclideanDistance(gArray[x].exprLvls, gArray[y].exprLvls)));
					}
				}
			}
		}
		
		float[] result = new float [dists.size()];
		for (int i=0; i<dists.size(); i++) {
			result[i] = ((Float)dists.get(i)).floatValue();
		}
		return result;		
	}
	
	public float[] bg_pdf;
	public float[] map_pdf;
	public float bg_min;
	public float bg_max;
	public float bin_size;
	public int num_bins;
	
	public void createPDFs() {
		float[] bg = sampleEuclidDists(0.02f);
		bg_min = Float.POSITIVE_INFINITY;
		bg_max = Float.NEGATIVE_INFINITY;
		for (int i=0; i<bg.length; i++) {
			if (bg[i] < bg_min) bg_min = bg[i];
			if (bg[i] > bg_max) bg_max = bg[i];
		}
		num_bins = Math.max(30, (int)((float)bg.length / 300));
		bin_size = (float)(bg_max - bg_min) / (float)num_bins;
		
		bg_pdf = new float [num_bins];
		for (int i=0; i<bg.length; i++) {
			bg_pdf[bin(bg[i])] += 1;
		}
		
		float[] map = mappedEuclidDists();
		map_pdf = new float [num_bins];
		for (int i=0; i<map.length; i++) {
			map_pdf[bin(map[i])] += 1;
		}
		
		for (int i=0; i<num_bins; i++) {
			bg_pdf[i] /= bg.length;
			map_pdf[i] /= map.length;
		}
	}
	
	public int bin (float value) {
		int result =  (int)((float)(value - bg_min) / bin_size);
		if (result < 0) return 0;
		if (result >= num_bins) return num_bins-1;
		return result;
	}
	
	public boolean genesMLagree(Gene g1, Gene g2) {
		float dist = euclideanDistance(g1.exprLvls, g2.exprLvls);
		if (map_pdf[bin(dist)] >= bg_pdf[bin(dist)])
			return true;
		else
			return false;
	}
	
	public void writeNewPclToStdOut() {
		System.out.print("YORF\tNAME\tGWEIGHT");
		for (int i = 0; i < exprNames.size(); i++) {
			System.out.print("\t" + (String) exprNames.get(i));
		}
		System.out.println();
		
		System.out.print("EWEIGHT\t\t");
		for (int i = 0; i < numExprs; i++) {
			System.out.print("\t1");
		}
		System.out.println();

		Set keySet = genes.keySet();
		String[] keys = new String[keySet.size()];
		keys = (String[]) keySet.toArray(keys);

		for (int i = 0; i < keys.length; i++) {
			Gene g = (Gene) genes.get(keys[i]);
			System.out.print(g.orf + "\t" + g.name + "\t1");
			DecimalFormat threePlaces = new DecimalFormat("0.000");
			for (int j = 0; j < g.exprLvls.length; j++) {
				System.out.print("\t"+threePlaces.format(g.exprLvls[j]));
			}
			System.out.println();
		}
	}

	public static void usage() {
		System.out.println("Usage: java -jar MedianMultiples.jar <dataset> <case_option>");
		System.out.println("\tdataset  - .pcl formatted file");
		System.out.println("\tmed_type - 0 = don't change gene names to upper case");
		System.out.println("\t           1 = change gene names to upper case");
	}

	public static void main(String[] args) {
		if (args.length < 2) {
			usage();
			System.exit(0);
		}

		MedianMultiples mm = new MedianMultiples();

		//Load in the .pcl file
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(args[0]));
			if (args[1].equalsIgnoreCase("0")){
			    mm.readDataFromPCLleaveCase(br);
			}
			else{
			    mm.readDataFromPCL(br);
			}
		}
		catch (FileNotFoundException e) {
			System.err.println("FileNotFound: " + args[0] + "\nStopping execution.");
			System.exit(0);
		}
		catch (IOException e) {
			System.err.println("Problem reading file: " + args[0]
					+ "\nStopping execution.");
			System.exit(0);
		}

		/*
		else {
			float d = Float.parseFloat(args[1]);
			mm.medianIfAgree(d);
		}*/
			mm.createPDFs();
			mm.meanAllThatAgree();
		
		mm.writeNewPclToStdOut();
		
		/*
		float rate = Float.parseFloat(args[1]);
		float[] dists = mm.sampleEuclidDists(rate);
		for (int i=0; i<dists.length; i++) {
			System.out.println(dists[i]);
		}
		
		dists = mm.mappedEuclidDists();
		for (int i=0; i<dists.length; i++) {
			System.err.println(dists[i]);
		}*/
		
	}
}
