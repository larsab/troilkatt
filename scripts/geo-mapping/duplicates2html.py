import sys

urlPrefix = "http://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc="

def getLink(gid):
    return '<a href="%s%s">%s</a>' % (urlPrefix, gid, gid)

if __name__ == '__main__':
    assert(len(sys.argv) == 4), 'Usage: %s duplicateFile outputFile title' % (sys.argv[0])

    lines = open(sys.argv[1]).readlines()
    
    fp = open(sys.argv[2], 'w')
    fp.write(
"""
<html>
<title>%s</title>
<body>
<p>%s</p>
<table border='1'>
<tr><td>Series/Datasets</td></tr>
""" % (sys.argv[3], sys.argv[3]))

    for l in lines:
        if l[0] == '#':
            continue
        parts = l.split('\t')
        fp.write("<tr><td>%s and %s</td></tr>" % (getLink(parts[0]), getLink(parts[1])))
    fp.write(
"""
</table>
</body>
</html>
""")
    fp.close()
