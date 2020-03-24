package tools;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;


public class DataInitialize{
	
	static FileSystem fs;
	static int  testFileCount=0;
	public static void main(String[] args) throws Exception {
		//BasicConfigurator.configure();
		fs=new Path(Global_Env.hdfsPath).getFileSystem(new Configuration());
		testFileCount=0;
		dataInitial(Global_Env.localFsInputPath);
		//text();
		System.out.println("-------------the number of test file: "+testFileCount);
		System.out.println("finished!!  ");
	}

	
	public static boolean isTestFile(){
		Random random=new Random();
		int randomNumber=random.nextInt(5002);
		if(randomNumber<=500){
			return true;
		}
		return false;
	}
	
	private static void text(){
		int num=0;
		for(int i=0;i<5000;i++){
			if(isTestFile())
				num++;
		}
		System.out.println("the number of test file "+num);
	}
	
	public static void dataInitial(String src) throws Exception{
		//fs.mkdirs(new Path(dst));
		File srcPath=new File(src);
		//System.out.println(src);
		File []srcFiles=srcPath.listFiles();
		if(srcFiles!=null){
			for(File file:srcFiles){
				if(file.isDirectory()){
					dataInitial(file.getAbsolutePath());
				}else{
					String inputFile=src+"/"+file.getName();
					String dstFile;
					if(isTestFile()){
						 dstFile=Global_Env.dataTextPath+"/"+file.getParentFile().getName()+"/"+file.getName();
						 testFileCount++;
						 System.out.println("choosen test file "+dstFile);
					}else{
					    dstFile=Global_Env.dataTrainPath+"/"+file.getParentFile().getName()+"/"+file.getName();
					}
					InputStream in=new BufferedInputStream(new FileInputStream(inputFile));
					OutputStream out=fs.create(new Path(dstFile));
					IOUtils.copyBytes(in, out, 1);
				}
					
			}
		}
		
	}


}
