import sys
from duplicates2html import getLink

if __name__ == '__main__':
    assert(len(sys.argv) == 5), 'Usage: %s subsetFile outputFile title label' % (sys.argv[0])

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
<tr><td>Subset</td><td>have N samples</td><td>in superset</td></tr>
""" % (sys.argv[3], sys.argv[4]))

    for l in lines:
        if l[0] == '#':
            continue
        parts = l.split('\t')
        fp.write("<tr><td>%s</td><td>%s</td><td>%s</td>" % (getLink(parts[0]), parts[2], getLink(parts[1])))
    fp.write("""</table>
</body>
</html>
""")
    fp.close()
