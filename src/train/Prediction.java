package train;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;

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

import prepare.CountDocType;
import prepare.TermCNumInputFromat;

import tools.Global_Env;
import tools.LoadPath;

public class Prediction extends Configured implements Tool{
	static HashMap<HashMap<String,String>,Double> hashMap=ConditionalProbility.getT_C();
	static HashMap<String,Double> priorProbilityMap=priorProbability.getPriorPrb();
	static List<String> classC=CountDocType.readDocType();
	
	
	public static void main(String[] args) {
//		for (int i = 0; i < classC.size(); i++) {
//            System.out.println(classC.get(i));  //.get(index)
//        }
		try {
			int res=ToolRunner.run(new Configuration(), new Prediction(),null);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.exit(1);
	}

	//Parameter 1--文件的二进制文件流
	//parameter 2--测试类名
	//return value--文件属于该类的概率
	static double conditionalProbabilityFroClass(BytesWritable content,String className){
		double conditionalProbability=priorProbilityMap.get(className);
		byte[] filBytes=content.getBytes();
		InputStream in=new ByteArrayInputStream(filBytes);
		BufferedReader br=new BufferedReader(new InputStreamReader(in));
		String term=null;
		try {
			while((term=br.readLine())!=null){
				if(term.toString().trim().equals("")){
					continue;
				}
				HashMap<String,String> key=new HashMap();
				key.put(className, term);
				if(hashMap.get(key)!=null)
				conditionalProbability+=Math.log(hashMap.get(key));
				else
					conditionalProbability+=Math.log(1.0/Global_Env.TOATAL_TERM);	
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return conditionalProbability;
	}
	

	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf=getConf();
		Job job=Job.getInstance(conf);
		FileSystem fileSystem=new Path(Global_Env.hdfsPath).getFileSystem(conf);
		
		job.setJobName("Prediction");
		job.setJarByClass(Prediction.class);
		
		FileInputFormat.addInputPath(job, new Path(Global_Env.seqTestOutputPath));
		new LoadPath(fileSystem, job).addOutPath(Global_Env.testResultPath);
		
		job.setInputFormatClass(TermCNumInputFromat.class);
		
		job.setMapperClass(Prediction.PredictionMapper.class );
		job.setReducerClass(Prediction.PreductionReduce.class);
		
		job.setOutputKeyClass(Text.class );
		job.setOutputValueClass(Text.class );
		
		System.out.println("___start_____");
		boolean res=job.waitForCompletion(true);
		
		System.out.println("finished!! res="+res);
		
		
		return 0;
	}
	
	public static class PredictionMapper extends Mapper<Object,Object,Text,Text>{
		private Text key_l=new Text();
		private BytesWritable value_l=null;
		//InputFormat <文件名，文件二进制流>
		//OutputFormat<文件名,<类名，文件属于该类的概率>>
		@Override
		public void map(Object docName, Object docContent,
				Mapper<Object,Object,Text,Text>.Context  context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			key_l=new Text();
			key_l.set(docName.toString());
			value_l=(BytesWritable)docContent;
			if(key_l.toString().trim()!="")
			//对于文档类集合中的每一个类，计算文档属于该类的概率
			for (int i = 0; i < Prediction.classC.size(); i++) {
				double probility=Prediction.conditionalProbabilityFroClass(value_l,Prediction.classC.get(i));
				Text val=new Text(Prediction.classC.get(i)+"\t"+probility);
				context.write(key_l, val);
	        }
		}
	}
	
	
	public static class PreductionReduce extends Reducer<Text,Text,Text,Text>{
		//InputFormat---<文件名,list<类名，文件属于该类的概率>>
		//OutputFormat---<文件名，最大的prob对应的类名>
		@Override
		public void reduce(Text key, Iterable<Text> values,
				Reducer<Text,Text,Text,Text >.Context  context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			double maxP=Double.NEGATIVE_INFINITY;
			String doctype="";
			for(Text value:values){
				String split[]=value.toString().trim().split("\\s+");
				if(split.length<2)
					continue;
				double currentP=Double.parseDouble(split[1]);
				if(currentP>maxP){
					maxP=currentP;
					doctype=split[0];
				}
			}
		//	System.out.println("reduce key:"+key+" value:"+count);
			context.write(key,new Text(doctype));
		}
	}

	
	
}
