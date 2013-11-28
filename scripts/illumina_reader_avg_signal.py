#!/usr/local/bin/python
import gzip
import sys
import ntpath
import math
import re
import os
import numpy
import scipy
from scipy import stats

def is_number(s):
	try:
		float(s)
		return True
	except ValueError:
		return False

def analyze_probe_line_2(s):
	samples = []
	pval = []
	pp = ["Pval", "p value", "p_value", "P Value", "P value", "pval", "P-VALUE", "P VALUE", "P-Value", "P-value"]
	exclude = ["Symbol", "SYMBOL", "Avg_NBEADS", "BEAD_STDERR", "BEAD_STDEV", "ARRAY_STDEV", "NARRAYS", "Detection-", \
	"MIN_Signal", \
	"MAX_Signal", "SEARCH_KEY", "ILMN_GENE", "CHROMOSOME", "DEFINITION", "SYNONYMS", "SPECIES", "SOURCE", \
	"TRANSCRIPT", "SOURCE_REFERENCE_ID", "REFSEQ_ID", "UNIGENE_ID", "ENTREZ_GENE_ID", "GI", "ACCESSION", \
	"PROTEIN_PRODUCT", "ARRAY_ADDRESS_ID", "PROBE_TYPE", "PROBE_START", "PROBE_SEQUENCE", \
	"PROBE_CHR_ORIENTATION", "PROBE_COORDINATES", "CYTOBAND", "ONTOLOGY_COMPONENT", "ONTOLOGY_PROCESS", \
	"ONTOLOGY_FUNCTION"]
	ids = ["ProbeID", "Probe ID", "PROBE_ID", "ID_REF", "Scan REF", "Array_Address_Id", "TargetID", "ID"]

	ex = []
        id_name = ""
        for i,j in s:
                i = i.strip(" ")
                m = False
                mt = re.match("(.*).COL.\d+", i)
                ishort = mt.group(1)
                for k in ids:
                        if k==ishort:
                                if id_name=="": id_name=i
                                m = True
                                break
                if m==True: continue
                for k in pp:
                        if k in ishort:
                                pval.append(i)
                                m = True
                                break
                if m==True: continue
                for k in exclude:
                        if k in ishort:
                                ex.append(i)
                                m = True
                                break
                if m==True: continue
                if ishort=="":
                        continue
                samples.append(i)
        return id_name, pval, ex, samples

def analyze_probe_line(s):
	samples = []
	pval = []
	pp = ["Pval", "p value", "p_value", "P Value", "P value", "pval", "P-VALUE", "P VALUE", "P-Value", "P-value"]
	avg_signal = ["AVG_Signal", "Avg_Signal"]
	ids = ["ProbeID", "Probe ID", "PROBE_ID", "ID_REF", "Scan REF", "Array_Address_Id", "TargetID", "ID"]
	ex = []
	id_name = ""
	for i,j in s:
                m = False
                mt = re.match("(.*).COL.\d+", i) #artificial attachment(*.COL.*)
                ishort = mt.group(1)
                for k in ids:
                        if k==ishort:
                                if id_name=="": id_name=i
                                m = True
                                break
                if m==True: continue
                for k in pp:
                        if k in ishort:
                                pval.append(i)
                                m = True
                                break
                if m==True: continue
                for k in avg_signal:
                        if k in ishort:
                                samples.append(i)
                                m = True
                                break
                if m==True: continue
                if ishort.rstrip(" ")=="":
                        continue

        if len(samples)==0:
                return "",[],[],[]

        return id_name, pval, ex, samples
	

def detect_sample_gsm_mapping(probes, samples, gsm, val_real, val_ref):
	map_sample_gsm = {}
	
	sample_to_gsm = {}
	for s in samples:
		sample_to_gsm[s] = {}
		for g in gsm:
			sample_to_gsm[s][g] = 0

	vm_real, vm_ref = {}, {}
	'''
	for a in probes:
		print val_real[a].keys()
		print val_ref[a].keys()
	'''
	good_probes = []
	for a in probes:
		if not val_real.has_key(a):
			#print "Error", a, "is skipped for val_real"
			continue
		if not val_ref.has_key(a):
			#print "Error", a, "is skipped for val_ref"
			continue

		stop = False
		for s in samples:
			if not val_real[a].has_key(s):
				#print "Error", s, "is skipped for val_real [", a, "]"
				stop = True
				break
		if stop:
			continue

		for g in gsm:
			if not val_ref[a].has_key(g):
				#print "Error", g, "is skipped for val_ref [", a, "]"
				#print "Error", a, "is skipped for val_ref"
				stop = True
				break
		if stop:
			continue

		good_probes.append(a)
		for s in samples:
			vm_real.setdefault(s, [])
			vm_real[s].append(val_real[a][s])
		for g in gsm:
			vm_ref.setdefault(g, [])
			vm_ref[g].append(val_ref[a][g])

	for g in gsm:
		zg = numpy.array(vm_ref[g])
		for s in samples:
			zs = numpy.array(vm_real[s])
			if zg.size != zs.size:
                        	print 'Array dimensions do not agree. Exiting'
				sys.exit(0)
			cor = scipy.stats.spearmanr(zg, zs)[0]
			sample_to_gsm[s][g] = cor
			#print s, g, cor
			
	print "FINAL"
	for s in samples:
		max_val = sample_to_gsm[s][gsm[0]]
		max_pos = 0
		for i,g in enumerate(gsm):
			#print sample_to_gsm[s][g],
			if sample_to_gsm[s][g] > max_val:
				max_val = sample_to_gsm[s][g]
				max_pos = i
		#print ""
		print s,gsm[max_pos],max_val
		map_sample_gsm[s] = gsm[max_pos]

	return map_sample_gsm

def read_illumina(n, metadir, GSMdir, outdir,platform):
	print n
	f = ""
	if n.endswith(".gz"):
		f = gzip.open(n)
	else:
		f = open(n)

	b = ntpath.basename(n)
	m = re.match("(GSE\d+)\D+", b)
	gse = m.group(1)

	print gse
	print platform
	platform_metadir = metadir + "/"+platform+"/" + gse
	if not os.path.exists(platform_metadir):
		print "Error Metadir: "+ platform_metadir  + " does not exist"
		sys.exit(1)

	gs = []
	fl = open(platform_metadir)
	for l in fl:
		l = l.rstrip("\n").split("|")[0]
		gs.append(l)
	fl.close()

	gsm = []
	val_ref = {}
	for g in gs:
		gsm_path = GSMdir + "/" + platform + "/" + g + "-tbl-1.txt"
		if not os.path.exists(gsm_path):
			print '%s doesn\'t exist' % gsm_path
			continue
		gsm.append(g)
		pa = open(gsm_path)
		xc = 0
		for l in pa:
			l = l.rstrip("\n").split("\t")
			if xc==0:
				ll = len(l)
				if (not ll==2) and (not ll==3):
					print "Error a lot of columns in GSM (not 2 or 3)"
					return
			ilum_probe = l[0]
			if l[1]=="": 
				continue
			val_ref.setdefault(ilum_probe, {})
			try:
				val_ref[ilum_probe][g] = float(l[1])
			except ValueError:
				print "Error", ilum_probe, g, l
				return
			xc+=1	
		pa.close()
		
	if not gsm:
		print 'None of GSM found in %s' % gsm_path
		sys.exit(0)
	print 'Found GSMs %s' % gsm	

	column_heading = []

	id_pos = 0
	val_pos = []

	while True:
		l = f.readline()
		if l=="": break
		l = l.rstrip("\n")
		l = l.rstrip("\r")
		if l.startswith("TargetID\tProbeID") or l.startswith("ID_REF\tSYMBOL") or \
		l.startswith("PROBE_ID\tSYMBOL"):
			column_heading = l.split("\t")
			if column_heading[-1]=="": column_heading = column_heading[:-1]
			last_pos = f.tell()

			next_line = f.readline().rstrip("\n").rstrip("\r").split("\t")
			if next_line[-1]=="": next_line = next_line[:-1]
                      	
			sys.stdout.write("NOTE: .COL.XXX is a suffix added to sample label on purpose for distinguishing samples!\n")

                        #NEW LINE ADDED OCT 8, 2013
                        for it, vt in enumerate(column_heading):
                                column_heading[it] = vt.rstrip(" ").lstrip(" ")
                                column_heading[it] += ".COL.%d" % it

			h = [c for c in column_heading]
			#for ih, vh in enumerate(h):
			#	h[ih] = vh.rstrip(" ").lstrip(" ")

			probe_line = zip(column_heading, next_line)
			id_name, pval, ex, samples = analyze_probe_line(probe_line)

			if id_name=="":
				id_name, pval, ex, samples = analyze_probe_line_2(probe_line)

			skipALine = False
			wc = 0
			for ne in next_line:
				if not is_number(ne):
					wc+=1
			wt = wc / float(len(samples))
			if wt>0.5:
				skipALine = True

			id_pos = -1
			for i,v in enumerate(h):
				if id_pos==-1 and v.strip() == id_name: 
					id_pos=i
					continue
				if v in set(samples): 
					val_pos.append(i)


			print id_name, id_pos
			if id_pos==-1:
				print "Error, id pos is -1", h
				return

			val = {}
			all_probes = set([])

			end_of_file = False
			f.seek(last_pos)
			all_p = {}

			if skipALine:
				f.readline()

			while True:
				l = f.readline()
				if l=="":
					end_of_file = True 
					break
				l = l.rstrip("\n").rstrip("\r").rstrip("\t").split("\t")

				id1 = l[id_pos]
				all_probes.add(id1)
				v = [float(l[x]) for x in val_pos]
				all_p[id1] = numpy.std(numpy.array(v))
				for ij, ik in zip(samples, v):
					val.setdefault(id1, {})
					val[id1][ij] = ik
				#sys.stdout.write("%s\t%s\n" % (id1, "\t".join(v)))

			all_p_it = all_p.items()
			all_p_it.sort(lambda x,y: cmp(x[1], y[1]), reverse=True)
			probe4000 = [x[0] for x in all_p_it][:4000]
		
			sg = detect_sample_gsm_mapping(probe4000, samples, gsm, val, val_ref)

			outfile = outdir + "/" + gse + "-" + platform + ".pcl"
			print outfile
			if os.path.exists(outfile):
				xt = 1
				while True:
					outfile = outdir + "/" + gse + "-" + platform + "_" + str(xt) + ".pcl"
					if not os.path.exists(outfile):
						break
					else:
						xt += 1

			fw = open(outfile, "w")

			fw.write("PROBE\t" + "\t".join([sg[s] for s in samples]) +"\n")

			for a in sorted(all_probes):
				tt = []
				for s in samples:
					tt.append("%.5f" % val[a][s])
				fw.write(a + "\t" + "\t".join(tt) + "\n")

			fw.close()

			if end_of_file==True:
				break
				
		elif l.startswith("ID_REF") or l.startswith("PROBE_ID") or l.startswith("ProbeID") or \
		l.startswith("Array_Address_Id") or l.startswith("Scan REF") or l.startswith("ID"):
			column_heading = l.split("\t")
			if column_heading[-1]=="": column_heading = column_heading[:-1]
			last_pos = f.tell()
			sys.stdout.write("NOTE: .COL.XXX is a suffix added to sample label on purpose for distinguishing samples!\n")
 
                        #NEW LINE ADDED OCT 8, 2013
                       	for it, vt in enumerate(column_heading):
                        	column_heading[it] = vt.rstrip(" ").lstrip(" ")
                                column_heading[it] += ".COL.%d" % it


			h = [c for c in column_heading]
#			for ih, vh in enumerate(h):
#				h[ih] = vh.rstrip(" ").lstrip(" ")

			next_line = f.readline().rstrip("\n").rstrip("\r").split("\t")
			if next_line[-1]=="": next_line = next_line[:-1]
			#print column_heading, next_line
			probe_line = zip(column_heading, next_line)
			id_name, pval, ex, samples = analyze_probe_line(probe_line)

			if id_name=="":
				id_name, pval, ex, samples = analyze_probe_line_2(probe_line)

			skipALine = False
			wc = 0
			for ne in next_line:
				if not is_number(ne):
					wc+=1
			wt = wc / float(len(samples))
			if wt>0.5:
				skipALine = True

			id_pos = -1
			for i,v in enumerate(h):
				if id_pos==-1 and v.strip() == id_name: 
					id_pos=i
					continue
				if v in set(samples): 
					val_pos.append(i)

			if id_pos==-1:
				print "Error, id pos is -1", h
				return

			#print id_name, id_pos
			#print pval
			#print ex
			#print samples

			all_probes = set([])
			val = {}

			end_of_file = False
			f.seek(last_pos)
			all_p = {}

			if skipALine:
				f.readline()

			while True:
				l = f.readline()
				if l=="":
					end_of_file = True 
					break
				l = l.rstrip("\n").rstrip("\r").rstrip("\t").split("\t")
				id1 = l[id_pos]
				all_probes.add(id1)
				try:
					v = [float(l[x]) for x in val_pos]
				except ValueError:
					print "Error",id1,samples
					return
				except IndexError:
					print "Error",id1,samples
					return
				all_p[id1] = numpy.std(numpy.array(v))
				for ij, ik in zip(samples, v):
					val.setdefault(id1, {})
					val[id1][ij] = ik
				#sys.stdout.write("%s\t%s\n" % (id1, "\t".join(["%.3f" % xj for xj in v])))

			all_p_it = all_p.items()
			all_p_it.sort(lambda x,y: cmp(x[1], y[1]), reverse=True)
			probe4000 = [x[0] for x in all_p_it][:4000]
			#print probe4000
		
			#print probe4000
			#print samples
			#print gsm	
			#print val
			#print val_ref
			sg = detect_sample_gsm_mapping(probe4000, samples, gsm, val, val_ref)

			outfile = outdir + "/" + gse + "-" + platform + ".pcl"
			if os.path.exists(outfile):
				xt = 1
				while True:
					outfile = outdir + "/" + gse + "-" + platform + "_" + str(xt) + ".pcl"
					if not os.path.exists(outfile):
						break
					else:
						xt += 1

			fw = open(outfile, "w")

			fw.write("PROBE\t" + "\t".join([sg[s] for s in samples]) +"\n")

			for a in sorted(all_probes):
				tt = []
				for s in samples:
					tt.append("%.5f" % val[a][s])
				fw.write(a + "\t" + "\t".join(tt) + "\n")

			fw.close()

			if end_of_file==True:
				break


if __name__=="__main__":
	print "Warning: do not repeatedly run this script on the same inputfile, it will automatically create duplicate output files such as '_1', '_2'..."
	if len(sys.argv)!=6:
		print "Usage: <inputfile> <metadir> <GSMdir> <outputdir> <platform>"
		sys.exit(1)
	inputfile = sys.argv[1]
	metadir = sys.argv[2]
	GSMdir = sys.argv[3]
	outputdir = sys.argv[4]
	platform = sys.argv[5]

	if not os.path.exists(inputfile):
		print inputfile + " does not exist"
		sys.exit(1)
	inputfile = os.path.abspath(inputfile)

	if not os.path.exists(metadir):
		print metadir + " does not exist"
		sys.exit(1)
	metadir = os.path.abspath(metadir)

	if not os.path.exists(GSMdir):
		print GSMdir + " does not exist"
		sys.exit(1)
	GSMdir = os.path.abspath(GSMdir)

	if not os.path.exists(outputdir):
		os.system("mkdir " + outputdir)
		print "Creating directory " + outputdir
	outputdir = os.path.abspath(outputdir)

	read_illumina(inputfile, metadir, GSMdir, outputdir, platform)
