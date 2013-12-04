if __name__ == '__main__':
    """
    Create a directory and verify its permission mode
    """
    import sys
    from troilkatt_setup import createLocalDir
    
    assert(len(sys.argv) == 3), "Usage %s directory mode" % (sys.argv[0])
    
    dir = sys.argv[1]
    modeStr = sys.argv[2]
    
    if createLocalDir(dir, modeStr) == False:
        sys.exit(-1)
    else:
        sys.exit(0)
    