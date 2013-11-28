#!/usr/local/bin/python
"""
Average values for duplicated genes, and maps them to Entrez IDs.
E.g. ILMN_1343291 to 1915
"""
import sys
import re
import os
import numpy as np

def read_gene_convert(n):
	"""
	Read file with mapping information for each platform
	e.g. 
	GPL6884	1	10
	GPL4133	4	9

	Return m tuple
	m.a - platform
	m.b - column
	m.c - column
	"""
	f = open(n)
	m = []
	for l in f:
		[a,b,c] = l.rstrip("\n").split("\t")
		m.append(tuple([a.upper(), int(b), int(c)]))
	f.close()
	return m

def read_pcl(n):
	f = open(n)
	header = f.readline().rstrip("\n")
	m = {}
	for l in f:
		l = l.rstrip("\n").split("\t", 1)
		if m.has_key(l[0]):
			sys.stderr.write("Error: this gene already appeared before: %s\n" % l[0])
			continue
		m[l[0]] = l[1]
	f.close()
	return header, m
	
def calc_average(ll):
	xs = []
	try:
		for l in ll:
			a = []
			for i in l.split("\t"):
				a.append(float(i))
			xs.append(a)
	except ValueError:
		print "Error, not a value!"
		return []

	a = np.array(xs)
	ma = np.mean(a, axis=0).tolist()
	aa = []
	for i in ma:
		aa.append("%.5f" % i)
	return aa
	
def consolidate(pcl, gpl):
	m = {}
	m_num = {}
	for g in pcl.keys():
		if not gpl.has_key(g): continue
		genes = gpl[g]
		for x in genes:
			m.setdefault(x, [])
			m[x].append(pcl[g])
	for g in m.keys():
		m_num[g] = calc_average(m[g])
		if m_num[g]==[]:
			return {}
	return m_num

"""
Command line arguments: %s <gene_convert_file> <GPL_dir> <input dir> <output dir> where
    gene_convert_file: file that contains GPL platforms and information in which column (in <platform>-tbl-1.txt entrez id will be found)
    GPL_dir: information about gene mappings for each platform
    input_file: input file 
    output_file: output file
 
"""

if __name__=="__main__":

	if len(sys.argv)!=5:
		sys.stderr.write("<gene_convert_file> <GPL_dir> <input file> <output file>\n")
		sys.stderr.write("Note: processes all files in the input dir.\n")
		sys.stderr.write("<GPL dir> contains the GPLXXXX-tbl-1.txt files.\n")
		sys.exit(1)

	pl = read_gene_convert(sys.argv[1])
	gpldir = sys.argv[2]
	input_file = sys.argv[3]
	output_file = sys.argv[4]
	
	genes = {}

	for (p, start, end) in pl:
		f = open(gpldir + "/" + p + "-tbl-1.txt")
		for l in f:
			l = l.rstrip("\n").split("\t")
			gene = l[start-1]
			entrez = l[end-1].split(" ")
			entrez_good = []
			for e in entrez:
				if e == "///": continue
				if e=="": continue
				e = e.upper()
				entrez_good.append(e)
			genes.setdefault(p, {})
			genes[p].setdefault(gene, [])
			genes[p][gene].extend(entrez_good)
		f.close()

	filename = os.path.basename(input_file)
	print 'Input filename: ' + filename
	m = re.match("(GSE\d+)-(GPL\d+).pcl", filename)
	if m is None: 
		print 'Incorrect input file name, can\'t retrieve GPL'
		exit
	gse_id = m.group(1)
	gpl_id = m.group(2)
	if not genes.has_key(gpl_id): 
		exit
	header, pcl = read_pcl(input_file)
	m_num = consolidate(pcl, genes[gpl_id])
	if len(m_num.keys())==0:
		print 'No results.'
		exit

	fw = open(output_file, "w")
	fw.write(header+"\n")
	for g in sorted(m_num.keys()):
		fw.write(g+ "\t" + "\t".join(m_num[g]) + "\n")
	fw.close()
	print 'Output wrote to %s' % output_file

