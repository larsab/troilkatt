#!/usr/bin/python

import re
import sys
from collections import deque
import os
import os.path

#must be a 1-channel sample
class Channel:
	def __init__(self, source, organism, characteristics, description):
		self.source = source
		self.organism = organism
		self.characteristics = characteristics
		self.description = description

class Sample:
	def __init__(self, num_channel, channel1, channel2, title, platform, sample_id, id_pos, value_pos):
		self.num_channel = num_channel
		self.channel1 = channel1
		self.channel2 = channel2
		#self.channel1 = Channel(channel1.source, channel1.organism, channel1.characteristics, channel1.description)
		#if channel2 is None:
		#	self.channel2 = None
		#else:
		#	self.channel2 = Channel(channel2.source, channel2.organism, channel2.characteristics, channel2.description)
		self.value_pos = value_pos
		self.id_pos = id_pos
		self.title = title
		self.sample_id = sample_id
		self.platform = platform

class Series:
	def __init__(self, title, samples, series_id):
		self.title = title
		self.samples = samples 
		self.series_id = series_id

def read_gse_xml(n):
	f = open(n)
	samples = []
	source = ""
	organism = ""
	characteristics = []
	sample_id = ""
	title = ""
	description = ""
	platform = ""
	channel_id = 1
	channel_count = 0
	sample_value_pos = ""
	sample_id_pos = ""
	a_channel_1 = None
	a_channel_2 = None

	series = []
	series_samples = []
	series_id = ""
	series_title = ""

	sample_open = False
	series_open = False

	while True:
		l = f.readline()
		if l=="": break
		l = l.rstrip("\n")
		l = l.strip()

		if l.startswith("<Sample iid="):
			m = re.match("<Sample iid=\"(.*)\">", l)
			sample_id = m.group(1)
			sample_open = True
			channel_count = 0
			channel_id = 1
			sample_value_pos = ""
			sample_id_pos = ""
			platform = ""
			a_channel_1 = None
			a_channel_2 = None
			title = ""
			continue

		if sample_open and l.startswith("<Title>"):
			m = re.match("<Title>(.*)</Title>", l)
			title = m.group(1)
			continue

		if sample_open and l.startswith("<Channel-Count>"):
			m = re.match("<Channel-Count>(\d+)</Channel-Count>", l)
			channel_count = int(m.group(1))
			a_channel_1 = None
			a_channel_2 = None
			continue

		if sample_open and l.startswith("<Channel position="):
			m = re.match("<Channel position=\"(\d+)\">", l)
			channel_id = int(m.group(1))
			characteristics = []
			source = ""
			organism = ""
			description = ""			
			continue

		if sample_open and l.startswith("<Source>"):
			m = re.match("<Source>(.*)</Source>", l)
			source = m.group(1)
			continue

		if sample_open and l.startswith("<Organism "):
			m = re.match("<Organism taxid=\"\d+\">(.*)</Organism>", l)
			organism = m.group(1)
			continue

		if sample_open and l.startswith("<Characteristics"):
			m = re.match("<Characteristics tag=\"(.*?)\">", l)
			if m is None:
				first_field = ""
			else:
				first_field = m.group(1)
			l = f.readline().rstrip("\n").strip()
			if l=="</Characteristics>":
				second_field = ""
			else:
				m = re.match("none\s+</Characteristics>", l)
				if m is not None:
					second_field = ""
				else:
					second_field = l
					f.readline()
			if first_field=="" and second_field=="":
				continue
			characteristics.append((first_field, second_field))
			continue

		if sample_open and l.startswith("<Data-Table>"):
			while True:
				l = f.readline().rstrip("\n").strip()
				if l=="</Data-Table>": 
					break
				if l.startswith("<Column position"):
					m = re.match("<Column position=\"(.*?)\">", l)
					pos1 = m.group(1)
					name = f.readline().rstrip("\n").strip()
					mm = re.match("<Name>(.*)</Name>", name)
					name = mm.group(1)
					if name=="VALUE":
						sample_value_pos = pos1
					elif name=="ID_REF":
						sample_id_pos = pos1
			continue
			
		if sample_open and l=="<Description>":
			description = ""
			while True:
				l = f.readline().rstrip("\n").strip()
				if l=="</Description>":
					break
				description = description + " " + l
				
			description = description.strip()
			continue

		if sample_open and l.startswith("</Channel>"):
			if channel_id==1:
				a_channel_1 = Channel(source, organism, characteristics, description)
			else:
				a_channel_2 = Channel(source, organism, characteristics, description)
			continue	

		if sample_open and l.startswith("<Platform-Ref ref"):
			m = re.match("<Platform-Ref ref=\"(.*?)\" />", l)
			platform = m.group(1)
			continue
	
		if sample_open and l.startswith("</Sample>"):
			#print a_channel_1, a_channel_2
			a_sample = Sample(channel_count, a_channel_1, a_channel_2, title, platform, sample_id, sample_id_pos, sample_value_pos)
			samples.append(a_sample)
			sample_open = False
			continue

		if l.startswith("<Series iid"):
			m = re.match("<Series iid=\"(.*)\">", l)
			series_id = m.group(1)
			series_title = ""
			series_samples = []
			series_open = True
			continue

		if series_open and l.startswith("<Title>"):
			m = re.match("<Title>(.*)</Title>", l)
			series_title = m.group(1)
			continue

		if series_open and l.startswith("<Sample-Ref ref"):
			m = re.match("<Sample-Ref ref=\"(.*?)\" />", l)
			series_samples.append(m.group(1))
			continue
		
		if series_open and l.startswith("</Series>"):
			a_series = Series(series_title, series_samples, series_id)
			series.append(a_series)
			series_open = False	

	return samples, series

if __name__=="__main__":
	if len(sys.argv) != 4:
		print "Error. Usage: <gpl_xml> <gpl_id> <output_parsed_dir>"
		sys.exit(0)

	gpl_id = sys.argv[2]
	s, se = read_gse_xml(sys.argv[1])
	output = sys.argv[3]

	gsm_series_map = {}
	for a in se:
		for sam in a.samples:
			gsm_series_map.setdefault(sam, [])
			gsm_series_map[sam].append(a.series_id)

        output_dir = output + '/'+ gpl_id
	if not os.path.exists(output_dir):
		os.system("mkdir %s" % output_dir)

	series_gsm_map = {}
	for a in s:
		if not gsm_series_map.has_key(a.sample_id): continue
		for ss in gsm_series_map[a.sample_id]:
			series_gsm_map.setdefault(ss, [])
			series_gsm_map[ss].append(a)
		
	for st in series_gsm_map.keys():
		fw = open("%s/%s" % (output_dir,st), "w")
		for a in series_gsm_map[st]:
			if a.platform!=gpl_id: continue
			strs = []
			strs.append(a.sample_id)
			#for combining GSM only=============
			strs.append(a.id_pos)
			strs.append(a.value_pos)
			#===================================
			strs.append(a.title)
			for j in range(a.num_channel):
				chan = None
				if j==0: chan = a.channel1
				elif j==1: chan = a.channel2
				if a.num_channel==2: 
					strs.append("Channel %d" % (j+1))
				strs.append(chan.source)
				for i in range(len(chan.characteristics)):
					if chan.characteristics[i][0]=="":
						strs.append("%s" % (chan.characteristics[i][1]))
					else:
						strs.append("%s: %s" % (chan.characteristics[i][0], chan.characteristics[i][1]))
				if chan.description=="": 
					continue
				else:
					strs.append("%s" % chan.description)
			ss = "|".join(strs)
			fw.write(ss+"\n")
                print 'Added file:  %s/%s' % (output_dir,st)   
		fw.close()
