package div;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class NameNodeIssue {
	
	public static void main(String[] args) throws IOException {
		String localFilename = "/home/larsab/troilkatt2/test-tmp/data/foo.txt";
		Path localPath = new Path(localFilename);
		Path remotePath = new Path("/user/larsab/foo");
		
		FileSystem hdfs = FileSystem.get(new Configuration());
		
		// Copy file to HDFS
		BufferedWriter os = new BufferedWriter(new FileWriter(new File(localFilename)));
		os.write("Foo bar baz\n");
		os.close();
		hdfs.copyFromLocalFile(false, true, localPath, remotePath);
		// Copy to local FS to create .crc file
		hdfs.copyToLocalFile(remotePath, localPath);
		
		// Modify file
		os = new BufferedWriter(new FileWriter(new File(localFilename)));
		os.write("Bar foo baz\n");
		os.close();
		
		// Copy modified file to HDFS
		// This will throw a Checksum error
		hdfs.copyFromLocalFile(false, true, localPath, remotePath);
		
		// hadoop fs -put /home/larsab/troilkatt2/test-tmp/data/foo.txt <anywhere> will also crash
		System.out.println("Done");
	}

}
