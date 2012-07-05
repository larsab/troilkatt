fp = open('gsm2gse.txt')

while 1:
    l = fp.readline()
    if l == '':
        break
    parts = l.split('\t')
    if len(parts) > 2:
        print '%s\t%d' % (parts[0], len(parts) - 1)

fp.close()
