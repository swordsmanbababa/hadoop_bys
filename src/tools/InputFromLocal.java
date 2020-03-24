package tools;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class InputFromLocal {
	public static Configuration conf;
	public static FileSystem fs;
	 public static void main(String[] args) throws Exception {
		try {
			conf=new Configuration();
			fs=FileSystem.get(URI.create(Global_Env.hdfsPath+"/"),conf);
			//problem:Below two lines run in one time will cause out-of-memory exception 
			//inputFromLocal("/home/hadoop/workspace/data/NBCorpus/Country","data/Country");
			inputFromLocal("/home/hadoop/workspace/data/NBCorpus/Industry","data/Industry");
			System.err.println("Load finished!!");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void inputFromLocal(String src,String dst) throws Exception{
		//fs.mkdirs(new Path(dst));
		File srcPath=new File(src);
		File []srcFiles=srcPath.listFiles();
		if(srcFiles!=null){
			for(File file:srcFiles){
				if(file.isDirectory()){
					inputFromLocal(file.getAbsolutePath(),dst+"/"+file.getName());
				}else{
					String inputFile=src+"/"+file.getName();
					String dstFile=Global_Env.hdfsPath+"/"+dst+"/"+file.getName();
					//System.out.println(dstFile);
					InputStream in=new BufferedInputStream(new FileInputStream(inputFile));
					OutputStream out=fs.create(new Path(dstFile));
					IOUtils.copyBytes(in, out, 1);
				}	
			}
		}
		
	}
}
