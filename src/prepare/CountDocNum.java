package prepare;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import tools.Global_Env;
import tools.LoadPath;


public class CountDocNum {
public static String outputPath="docCountOutput";
	static FileSystem fileSystem;
	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

	//	countDocNumber();
		Configuration conf=new Configuration();
		fileSystem=new Path(outputPath).getFileSystem(conf);
		Job job=Job.getInstance(conf,"CountDocNum");
		
		job.setInputFormatClass(DocNumInputFormat.class);
		
		job.setJarByClass(CountDocNum.class);
		
		job.setMapperClass(CountDocNum.CountDocNumMapper.class);
		job.setCombinerClass(CountDocNum.CountDocNumReduce.class);
		job.setReducerClass(CountDocNum.CountDocNumReduce.class);
		
		job.setOutputKeyClass(Text.class );
		job.setOutputValueClass(IntWritable.class );
		
		
		//FileInputFormat.addInputPath(job, new Path(inputPath));
		//new LoadPath(fileSystem, job).addInPath(new Path(Global_Env.dataTrainPath));
		FileInputFormat.addInputPath(job, new Path(Global_Env.seqOutputPath));
		if(fileSystem.exists(new Path(outputPath)))
		{
			fileSystem.delete(new Path(outputPath));
		}
		FileOutputFormat.setOutputPath(job, new Path(outputPath ));
		
		System.out.println("___start_____");
		boolean res=job.waitForCompletion(true);
		
		System.out.println("finished!! res="+res);
		System.exit(1);
		
		
		
	}
	
	public static int countDocNumber(){
		String path="hdfs://localhost:9000/user/hadoop/"+outputPath+"/part-r-00000";
		int count=0;
		Configuration conf=new Configuration();
		try {
			FileSystem fs=FileSystem.get(URI.create(path),conf);
			InputStream in=null;
			in=fs.open(new Path(path));
			BufferedReader br=new BufferedReader(new InputStreamReader(in));
			String line=null;
			while((line=br.readLine())!=null){
				count++;
				System.out.println("line :"+line);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Doc num="+count);
		return count;
	}
	
	



	public static class CountDocNumMapper extends Mapper<Text,IntWritable,Text,IntWritable>{
		
		public CountDocNumMapper() {
		
		}

		@Override
		public void map(Text key, IntWritable value,
				Mapper<Text,IntWritable,Text,IntWritable>.Context  context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
//			if(key.toString().trim().equals(""))
//				System.out.println("map key:"+key+" value:"+value);
			context.write(key, value);
		}
		
		

	}
	
//	public static class CountDocNumMapper extends Mapper<Object,Object,Text,IntWritable>{
//		
//		public CountDocNumMapper() {
//		
//		}
//
//
//		public void map(Object key, Object value,
//				Mapper<Object,Object,Text,IntWritable>.Context  context)
//				throws IOException, InterruptedException {
//			// TODO Auto-generated method stub
//			System.out.println("map key:"+key+" value:"+value);
//			//context.write(key, value);
//		}
//		
//		
//
//	}
	
	public static class CountDocNumReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
		
		public CountDocNumReduce() {
		
		}

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text,IntWritable,Text,IntWritable >.Context  context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int count=0;
			for(IntWritable value:values){
				count+=value.get();
			}
//			if(key.toString().trim().equals(""))
//			System.out.println("reduce key:"+key+" value:"+count);
			context.write(key, new IntWritable(count));
		}

	
	}


	private static void addInPath(Job job,Path path) throws IOException {
		// TODO Auto-generated method stub
		FileStatus[] files = fileSystem.listStatus(path);
		for(int i=0;i<files.length;i++){
			if(files[i].isDirectory()){
				
				addInPath(job,files[i].getPath());
				
			}else{
				//suffix 
				System.out.println("add path"+files[i].getPath().getParent());
				FileInputFormat.addInputPath(job,files[i].getPath().getParent());
				break;
			}
		}
		
	}
	
}
