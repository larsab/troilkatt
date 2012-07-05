import sys
from duplicates2html import getLink

if __name__ == '__main__':
    assert(len(sys.argv) == 5), 'Usage: %s mappingFile outputFile title label' % (sys.argv[0])

    lines = open(sys.argv[1]).readlines()
    lines.sort()
    
    fp = open(sys.argv[2], 'w')
    fp.write(
"""
<html>
<title>%s</title>
<body>
<p>%s<p>
<table border='1'>
""" % (sys.argv[3], sys.argv[4]))

    for l in lines:
        if l[0] == '#':
            continue
        parts = l.split('\t')
        fp.write("<tr><td>%s</td><td>" % (getLink(parts[0])))
        fp.write('%s' % (getLink(parts[1])))
        for p in parts[2:]:
            fp.write(', %s' % (getLink(p)))
        fp.write('</td></tr>')
    fp.write(
"""</table>
</body>
</html>
""")
    fp.close()
