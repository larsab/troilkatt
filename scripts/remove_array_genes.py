#!/usr/local/bin/python
import numpy as np
import sys
import re
import math

def read_gse(n, o):
	f = open(n)
	gsm = f.readline().rstrip("\n").split("\t")[1:]
	m = []
	counts = []
	by_array = []

	for i in gsm:
		counts.append(0)
		by_array.append([])

	for l in f:
		l = l.rstrip("\n").split("\t")
		m.append(l)
	f.close()

	if len(m)==0:
		sys.stderr.write("Dataset %s is skipped!\n" % n)
		return False

	for x in m:
		g = x[0]
		v = x[1:]
		for e,i in enumerate(v):
			if i=="":
				counts[e]+=1
			else:
				by_array[e].append(float(i))

	takeLog = 0
	for i in range(len(gsm)):
		a = np.array(by_array[i])
		xstdev = np.std(a)
		xmean = np.mean(a)
		sys.stderr.write("%s %.3f %.3f\n" % (gsm[i], xmean, xstdev))
		if xmean>10:
			takeLog+=1
	
	toLog = False
	if takeLog > int(len(gsm) * 0.5):
		toLog = True

	threshold = int(len(m) * 0.3)
	delete = []

	for i in range(len(counts)):
		if counts[i]>threshold:
			delete.append(i)
			sys.stderr.write("In Dataset %s, %s is deleted because not enough genes!\n" % (n, gsm[i]))
			#return False
	
	if len(gsm) - len(delete) <= 2:
		sys.stderr.write("Dataset %s is skipped because not enough columns!\n" % n)
		return False

	g_threshold = int((len(gsm) - len(delete)) * 0.3)
	delete_set = set(delete)

	fw = open(o, "w")
	fw.write("gene\t%s\n" % "\t".join([g for i,g in enumerate(gsm) if i not in delete_set]))
	type1err = []
	type2err = []
	type3err = []

	for x in m:
		g = x[0]
		v = x[1:]
		vv = []
		for e,i in enumerate(v):
			if e in delete_set: continue
			if i=="":
				vv.append(i)
				continue
			if toLog: #if taking log
				fi = float(i)
				if fi>=0 and fi<=1:
					if len(type1err)<200:
						type1err.append("Gene %s value %.3f becomes 0" % (g, fi))
					vv.append("0")
				elif fi>=-1 and fi<=0:
					if len(type2err)<200:
						type2err.append("Gene %s value %.3f becomes 0" % (g, fi))
					vv.append("0")
				elif fi<-1:
					if len(type3err)<200:
						type3err.append("Gene %s large negative value %.3f becomes -log(abs)" % (g, fi))
					vv.append("%.3f" % (-1.0 * math.log(-1.0 * float(i), 2)))
				else:
					vv.append("%.3f" % (math.log(float(i), 2)))
			else: #assume log-transformed already
				vv.append(i)
		c = 0
		for i in vv:
			if i=="": c+=1
		if c>g_threshold:
			sys.stderr.write("Gene %s is skipped\n" % g)
			continue
		fw.write("%s\t%s\n" % (g, "\t".join(vv)))
	fw.close()

	sys.stderr.write("\n".join(type1err))
	sys.stderr.write("\n".join(type2err))
	sys.stderr.write("\n".join(type3err))

	return True

if __name__=="__main__":
	if len(sys.argv)!=3:
		print "Usage <input file> <output file>"
		sys.exit(1)

	print sys.argv[1]
	r = read_gse(sys.argv[1], sys.argv[2])
	if r:
		sys.exit(0)
	else:
		sys.exit(1)
