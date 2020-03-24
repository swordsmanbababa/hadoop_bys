package tools;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LoadPath {
	FileSystem fileSystem;
	Job job;
	
	public LoadPath(FileSystem fileSystem, Job job) {
		this.fileSystem = fileSystem;
		this.job = job;
	}

	public  void addRInPath(Path path) throws IOException {
		// TODO Auto-generated method stub
		FileStatus[] files = fileSystem.listStatus(path);
		for(int i=0;i<files.length;i++){
			if(files[i].isDirectory()){
				
				addRInPath(files[i].getPath());
				
			}else{
				//suffix 
				System.out.println("add path: "+files[i].getPath().getParent());
				FileInputFormat.addInputPath(job,files[i].getPath().getParent());
				break;
			}
		}
		
	}
	
	public  void addInPath(Path path) throws IOException {
		//FileInputFormat.addInputPath(job,new Path("data/Country/AFGH"));
		//FileInputFormat.addInputPath(job,new Path("data/Country/SYRIA"));
		System.out.println("add path"+path);
		FileInputFormat.addInputPath(job,path);
	}
	
	public void addOutPath(String path){
		try {
			if(fileSystem.exists(new Path(path)))
			{
				fileSystem.delete(new Path(path));
			}
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		FileOutputFormat.setOutputPath(job, new Path(path));
		
	}
}
