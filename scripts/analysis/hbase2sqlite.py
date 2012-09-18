import os

if __name__ == '__main__':
    import sys

    assert(len(sys.argv) == 3), 'Usage: python %s hbase-table output-filename' % (sys.argv[0])
    tableName = sys.argv[1]
    outputFilename = sys.argv[2]
    
    if tableName.find('troilkatt-'):
        print 'Table name does not start with troilkatt-'
        sys.exit(1)

    hdfsDir = 'troilkatt/export/%s' % (tableName)
    rmdirCmd = 'hadoop fs -rmr %s' % (hdfsDir)
    exportCmd = 'hadoop jar $HBASE_HOME/hbase.jar export %s %s' % (tableName, hdfsDir)
    copyCmd = 'hadoop fs -getmerge %s %s' % (hdfsDir, outputFilename)

    print 'Deleting hadoop directory: %s' % (hdfsDir)
    print rmdirCmd

    print 'Export hbase table %s to HDFS dir %s' % (tableName, hdfsDir)
    print exportCmd

    print 'Copying files from HDFS dir %s to local file %s' % (hdfsDir, outputFilename)
    print copyCmd

    print 'Deleting hadoop directory: %s' % (hdfsDir)
    print rmdirCmd
