package BigData.BD_Assignment_1;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;


public class FileUpload {
	
	public final List<String> srcUrlLst = new ArrayList<String>();

	public static void main(String[] args) {

		if (null == args || args.length <= 0) {
			System.out.println("Please enter Destination folder to upload the files!");
			return;
		}
		String dstPath = args[0];
		FileUpload uploadFile = new FileUpload();
		uploadFile.uploadFileToServer(dstPath);
	}

	public void uploadFileToServer(String dstPath) {
		String localSrc = "";
		InputStream in = null;
		OutputStream out = null;
		String fileName = "";

		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));

		try {
			CompressionCodecFactory factory = new CompressionCodecFactory(conf);			
			getUrl(conf);//get all the file urls in list
			for (int i = 0; i < srcUrlLst.size(); i++) {
				localSrc = srcUrlLst.get(i);
				fileName = FilenameUtils.getName(srcUrlLst.get(i));
				URL fileUrl = new URL(localSrc);
				in = new BufferedInputStream(fileUrl.openStream());
				Scanner sc = null;
				FileSystem fs = FileSystem.get(conf);
				String destUri = dstPath + fileName;
				Path filePath = new Path(destUri);
				String s="Y";
				if (fs.exists(filePath)) {
					
					sc = new Scanner(System.in);
					System.out.println("###File " + filePath + " already exists on server!!");
					System.out.println("Do you want to download again? (Y/N)");
					s = sc.nextLine();					
				}
				if(s.equalsIgnoreCase("Y")){
					//Copy zip file to server				
					Path outPath = new Path(destUri);
					out = fs.create(outPath);
					IOUtils.copyBytes(in, out, 4096, true);
					System.out.println("***File " + filePath + " copied to server!!***");
					
					//De-compress bz2 file
					CompressionCodec codec = factory.getCodec(outPath);
					if (codec == null) {
					    System.err.println("No codec found for " + destUri);
					    System.exit(1);
					}
					String outUri =
						    CompressionCodecFactory.removeSuffix(dstPath + fileName, codec.getDefaultExtension());

						in = codec.createInputStream(fs.open(outPath));
						out = fs.create(new Path(outUri));
						IOUtils.copyBytes(in, out, conf);
				}
				
			}

		} catch (FileNotFoundException e) {
			System.out.println("Method uploadFileToServer : File not Found!!!" + e.getMessage());
		} catch (IOException e) {
			System.out.println("Method uploadFileToServer:IO Exception!!!" + e.getMessage());
		} finally {
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
		}

	}

	public void getUrl(Configuration conf) {

		try {
			srcUrlLst.add("http://www.utdallas.edu/~axn112530/cs6350/lab2/input/20417.txt.bz2");
			srcUrlLst.add("http://www.utdallas.edu/~axn112530/cs6350/lab2/input/5000-8.txt.bz2");
			srcUrlLst.add("http://www.utdallas.edu/~axn112530/cs6350/lab2/input/132.txt.bz2");
			srcUrlLst.add("http://www.utdallas.edu/~axn112530/cs6350/lab2/input/1661-8.txt.bz2");
			srcUrlLst.add("http://www.utdallas.edu/~axn112530/cs6350/lab2/input/972.txt.bz2");
			srcUrlLst.add("http://www.utdallas.edu/~axn112530/cs6350/lab2/input/19699.txt.bz2");
		} catch (Exception e) {
			System.out.println("Exception in method getUrl!!!" + e.getMessage());
			
		}
	}

}
