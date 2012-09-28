import os

if __name__ == '__main__':
    import sys

    assert(len(sys.argv) >= 4), 'Usage: python %s <hbase-table> <output-filename> <column1> [column2 ...]' % (sys.argv[0])
    tableName = sys.argv[1]
    outputFilename = sys.argv[2]
    
    cols = None
    for c in sys.argv[3:]:
        if len(c.split(':')) != 2:
            print 'Invalid argument: column names should be in the format "family:qualifier": %s' % (c)
            sys.exit(-1)
        if cols == None:
            cols = c
        else:
            cols = cols + ' ' + c
    
    if tableName.find('troilkatt-'):
        print 'Table name does not start with troilkatt-'
        sys.exit(1)

    hdfsDir = 'troilkatt/export/%s' % (tableName)
    rmdirCmd = 'hadoop fs -rmr %s' % (hdfsDir)
    exportCmd = 'hadoop jar $TROILKATT_HOME/lib/troilkatt.jar edu.princeton.function.troilkatt.mapreduce.HbaseAsciiExport %s %s %s' % (tableName, hdfsDir, cols)
    copyCmd = 'hadoop fs -getmerge %s %s' % (hdfsDir, outputFilename)

    print 'Deleting hadoop directory: %s' % (hdfsDir)
    print rmdirCmd

    print 'Export hbase table %s to HDFS dir %s' % (tableName, hdfsDir)
    print exportCmd

    print 'Copying files from HDFS dir %s to local file %s' % (hdfsDir, outputFilename)
    print copyCmd

    print 'Deleting hadoop directory: %s' % (hdfsDir)
    print rmdirCmd
