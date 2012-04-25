import sys, os

assert(len(sys.argv) == 6), 'Usage: pcl2qdab.py SLEIPNIR.DIR INPUT.FILE DAT.FILE QDAB.FILE QUANT.FILE'

sleipnirDir = sys.argv[1]
inputFile = sys.argv[2]
datFile = sys.argv[3]
qdabFile = sys.argv[4]
quantFile = sys.argv[5]

#troilkattContainerCmd = '/nhome/larsab/troilkatt/apps/bin/troilkatt-container 8 -1'

pcl2datCmd = '%s/Distancer -i %s -o %s' % (sleipnirDir, inputFile, datFile)
dat2qdabCmd = '%s/Dat2Dab -i %s -q %s -o %s' % (sleipnirDir, datFile, quantFile, qdabFile)

print 'Execute: ' + pcl2datCmd
os.system(pcl2datCmd)
print 'Execute: ' + dat2qdabCmd
os.system(dat2qdabCmd)
os.remove(datFile)
