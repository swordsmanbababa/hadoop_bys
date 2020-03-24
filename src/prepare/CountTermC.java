package prepare;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tools.Global_Env;
import tools.LoadPath;

//统计类中单词频率
//CountTermC.java
public class CountTermC extends Configured implements Tool{
	

	private static String outputPath="TermCNumOutput";
	public static void main(String[] args) throws Exception {
		int res=ToolRunner.run(new Configuration(), new CountTermC(),null);
		System.exit(1);
//		countTermNumber();
	}
	
	//统计测试集合内所有单词数
	public static int countTermNumber(){
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
				String[] splited = line.split("\\s+");
				int termNum=Integer.parseInt(splited[2]);
				count+=termNum;
//				System.out.println("line :"+line);
//				System.out.println(""+splited[0]);
//				System.out.println(""+splited[1]);
//				System.out.println(""+splited[2]);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Term num="+count);
		return count;
	}
	

	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf=getConf();
		Job job=Job.getInstance(conf);
		FileSystem  fileSystem=new Path(Global_Env.hdfsPath).getFileSystem(conf);
		
		try{
			job.setJobName("Count-TermC-Type");
			job.setJarByClass(CountTermC.class);
			
			//new LoadPath(fileSystem,job).addRInPath(new Path(Global_Env.dataTrainPath));
			FileInputFormat.addInputPath(job, new Path(Global_Env.seqOutputPath));
			new LoadPath(fileSystem, job).addOutPath(outputPath);
		
			job.setInputFormatClass(TermCNumInputFromat.class);
			
			job.setMapperClass(CountTermC.CountTermcSeqNumMapper.class);
			job.setCombinerClass(CountTermC.CountTermCNumReduce.class);
			job.setReducerClass(CountTermC.CountTermCNumReduce.class);
			
			job.setOutputKeyClass(Text.class );
			job.setOutputValueClass(IntWritable.class );
			
			System.out.println("___start_____");
			boolean res=job.waitForCompletion(true);
			
			System.out.println("finished!! res="+res);
			
			
		}
		finally{
			
		}
		return 0;
	}
	


//	public static class CountTermcNumMapper extends Mapper<Text,Text,HashMap<Text,Text>,IntWritable>{
//		
//	
//
//		@Override
//		public void map(Text line, Text value,
//				Mapper<Text,Text,HashMap<Text,Text>,IntWritable>.Context  context)
//				throws IOException, InterruptedException {
//			// TODO Auto-generated method stub
//			System.out.println("map key:"+line+" value:"+value);
//			System.out.println("map___________");
//			HashMap<Text,Text> key=new HashMap<Text,Text>();
//			key.put(line, value);
//			context.write(key, new IntWritable(1));
//		}
//		
//		
//
//	}
	
	public static class CountTermcNumMapper extends Mapper<Object,Object,Text,IntWritable>{
		@Override
		public void map(Object line, Object value,
				Mapper<Object,Object,Text,IntWritable>.Context  context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Text key=new Text();
			key.set(line.toString()+"/t"+value.toString());
			context.write(key, new IntWritable(1));
		}
	
	}
	
	public static class CountTermcSeqNumMapper extends Mapper<Object,Object,Text,IntWritable>{
		private Text key_l=new Text();
		private BytesWritable value_l=null;
		
		//InputFormat <className,binary file stream>
		//OutputFormat<<classNmae,word>,count>
		@Override
		public void map(Object line, Object value,
				Mapper<Object,Object,Text,IntWritable>.Context  context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			key_l=new Text();
			key_l.set(line.toString());
			value_l=(BytesWritable)value;
			//read each line of file write key-value
			writeKVInLine(context);	
		}
		
		private void writeKVInLine(Mapper<Object,Object,Text,IntWritable>.Context  context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			byte[] filebytes=value_l.getBytes();
			InputStream in = new ByteArrayInputStream(filebytes);
			BufferedReader brIn=new BufferedReader(new InputStreamReader(in));
			
			String key_word=null;
		
			while((key_word=brIn.readLine())!=null){
				if(key_word.toString().trim().equals(""))
					continue;
				Text key_w=new Text(key_l.toString()+"\t"+new String(key_word));
				context.write(key_w, new IntWritable(1));
			}
		}
	}
	
	
    //Reduce
    //输入<<类名，单词>，{count1，count2，...,countn}>
    //输出<<类名，单词>，单词数量>	
public static class CountTermCNumReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
		
	

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text,IntWritable,Text,IntWritable >.Context  context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int count=0;
			for(IntWritable value:values){
				count+=value.get();
			}
		//	System.out.println("reduce key:"+key+" value:"+count);
			context.write(key, new IntWritable(count));
		}

	
	}
}
