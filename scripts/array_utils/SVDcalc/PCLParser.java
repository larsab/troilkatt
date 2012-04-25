import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class PCLParser {

	public String[]   idx2gene;
	public HashMap    gene2idx;
	public String[]   conds;
	public double[][] data;
	public int        numExprs;
	public int				 numGenes;
	
	public PCLParser () {
		gene2idx = new HashMap();
	}
	
	public boolean readData(String filename) throws IOException {
		BufferedReader fin = new BufferedReader(new FileReader(filename));
		return readDataFromStream(fin);
	}
	
	public boolean readDataFromStream(BufferedReader fin) throws IOException {
		numExprs = 0;
		numGenes = 0;

		ArrayList exprNames = new ArrayList();
		ArrayList geneList  = new ArrayList();
		ArrayList dataVecs  = new ArrayList();
		
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
					
					conds = new String [exprNames.size()];
					conds = (String[]) exprNames.toArray(conds);
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
						String gName = lineParts[1].toUpperCase();
						gene2idx.put(gName, new Integer(numGenes));
						geneList.add(numGenes, gName);
						
						double[] exprVec = new double [numExprs];
						dataVecs.add(numGenes, exprVec);
						for (int e = 0; e < numExprs; e++) {
							exprVec[e] = 0;
						}
						
						numGenes++;
						
						//Columns from 2 to the end are data points (except the
						// gweightCol)
						for (int i = 2; i < lineParts.length; i++) {
							if (i != gweightCol) {
								//Try to convert the string to a number, if you
								// fail, just fill in with 0s
								try {
									double val = Double.parseDouble(lineParts[i]);
									if (!Double.isNaN(val)) {									
										exprVec[i - colOffset] = val;
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
			}
		}
		catch (IOException except) {
			throw (except);
		}
		
		//Convert genes and data to arrays
		idx2gene = new String [geneList.size()];
		idx2gene = (String[]) geneList.toArray(idx2gene);
		
		data = new double[numGenes][numExprs];
		for (int g=0; g<numGenes; g++) {
			data[g] = (double[])dataVecs.get(g);
		}
			
		//Finally, return success
		return true;
	}


}
