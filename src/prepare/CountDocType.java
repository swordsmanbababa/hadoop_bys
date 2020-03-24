package prepare;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




import tools.Global_Env;
import tools.LoadPath;

//计算每类文件的数目
public class CountDocType extends Configured implements Tool{
	
	private CountDocType() {}
	private static String trainoutputPath="DocTypeNumOutput";
	private static String wholeOutputPath="DocTypeWholeOutput";
	private static String testDocTypeNumOutput="testDocTypeNumOutput";
	public static void main(String[] args) throws Exception {
		//BasicConfigurator.configure();
//		readTestDocType();
//		readDocType();
		
		int res=ToolRunner.run(new Configuration(), new CountDocType(),null);
		System.exit(1);
	}
	

	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf=getConf();
		Job job=Job.getInstance(conf);
		FileSystem  fileSystem=new Path(Global_Env.hdfsPath).getFileSystem(conf);
		
		try{
			job.setJobName("Count-Doc-Type");
			job.setJarByClass(CountDocType.class);
			
			//new LoadPath(fileSystem,job).addRInPath(new Path(Global_Env.inputPath));
//			FileInputFormat.addInputPath(job, new Path(Global_Env.seqTestOutputPath));
//			new LoadPath(fileSystem, job).addOutPath(testDocTypeNumOutput);
			
//			FileInputFormat.addInputPath(job, new Path(Global_Env.seqOutputPath));
//			new LoadPath(fileSystem, job).addOutPath(outputPath);
			
			FileInputFormat.addInputPath(job, new Path(Global_Env.WholeOutputPath));
			new LoadPath(fileSystem, job).addOutPath(trainoutputPath);
			
		
			
			job.setInputFormatClass(DocTypeInputFormat.class);
			
			job.setMapperClass(CountTypeNumMapper.class);
			job.setCombinerClass(CountTypeNumReduce.class);
			job.setReducerClass(CountTypeNumReduce.class);
			
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
	
	//InputFormat---<文件所在的目录类别，1>
	//OnputFormat---<文件所在的目录类别，1>
	//该map函数对key-value不做处理
	public static class CountTypeNumMapper extends Mapper<Text,IntWritable,Text,IntWritable>{
		@Override
		public void map(Text key, IntWritable value,
				Mapper<Text,IntWritable,Text,IntWritable>.Context  context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//System.out.println("map key:"+key+" value:"+value);
			context.write(key, value);
		}
	}
	
	//InputFormat---<文件所在的目录类别，1>
	//OnputFormat---<文件所在的目录类别，该类别下文件总数>
	public static class CountTypeNumReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
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
	
	//读取输出文件，统计文件类型,返回list<文件类型>
	public static List readDocType(){
		List<String> list=new ArrayList();
		 Configuration conf = new Configuration();
        String path = trainoutputPath+"/part-r-00000";
        FileSystem fs;
        int totalNum=0;
		try {
			fs = FileSystem.get(URI.create(path),conf);
			InputStream in=null;
			in=fs.open(new Path(path));
			BufferedReader br=new BufferedReader(new InputStreamReader(in));
			String line=null;
			while((line=br.readLine())!=null){
				String[] split=line.split("\\s+");
				String docType=split[0];
				String docNum=split[1];
				totalNum+=Integer.parseInt(docNum);
				list.add(docType);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("total doc number="+totalNum);
		return list;
	}
	
	public static List readTestDocType(){
		List<String> list=new ArrayList();
		 Configuration conf = new Configuration();
        String path = testDocTypeNumOutput+"/part-r-00000";
        FileSystem fs;
        int totalNum=0;
		try {
			fs = FileSystem.get(URI.create(path),conf);
			InputStream in=null;
			in=fs.open(new Path(path));
			BufferedReader br=new BufferedReader(new InputStreamReader(in));
			String line=null;
			while((line=br.readLine())!=null){
				String[] split=line.split("\\s+");
				String docType=split[0];
				String docNum=split[1];
				totalNum+=Integer.parseInt(docNum);
				System.out.println("total doc number="+totalNum);
				list.add(docType);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return list;
	}

}
