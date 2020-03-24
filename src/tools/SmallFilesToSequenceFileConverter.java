package tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import train.Prediction;

public class SmallFilesToSequenceFileConverter extends Configured implements Tool{
	public static void main(String[] args) throws IOException {
	//	read(Global_Env.seqOutputPath);
		
		int exitCode;
		try {
			exitCode = ToolRunner.run(new Configuration() ,new SmallFilesToSequenceFileConverter(), null);
			System.exit(exitCode);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		FileSystem  fileSystem=new Path(Global_Env.hdfsPath).getFileSystem(getConf());
		Job job=Job.getInstance(getConf());
		
		job.setJobName("SmallFilesToSequenceFileConverter");
		job.setJarByClass(SmallFilesToSequenceFileConverter.class);
		
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setJarByClass(SmallFilesToSequenceFileConverter.class );
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		
		job.setMapperClass(SequenceFileMapper.class);

		
//		new LoadPath(fileSystem,job).addRInPath(new Path(Global_Env.dataTrainPath));
//		new LoadPath(fileSystem, job).addOutPath(Global_Env.seqOutputPath);
		
		new LoadPath(fileSystem,job).addRInPath(new Path(Global_Env.dataTextPath));
		new LoadPath(fileSystem, job).addOutPath(Global_Env.seqTestOutputPath);
		
		
//		new LoadPath(fileSystem,job).addRInPath(new Path(Global_Env.dataTrainPath));
//		new LoadPath(fileSystem,job).addRInPath(new Path(Global_Env.dataTextPath));
//		new LoadPath(fileSystem, job).addOutPath(Global_Env.WholeOutputPath);
		

		
		System.out.println("___start_____");
		boolean res=job.waitForCompletion(true);
		
		System.out.println("finished!! res="+res);
		
		return res==true?1:0;
	}
	

    @SuppressWarnings("deprecation")
    public static void read(String pathStr) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(pathStr);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);

        Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
        while (reader.next(key, value)) {
            System.out.printf("%s\t%s\n", key, value);
        }
        IOUtils.closeStream(reader);
    }

	
	static class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable >{
		private Text fileNameKey;
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			InputSplit split=context.getInputSplit();
			String path=((FileSplit)split).getPath().getParent().getName()+"/"+((FileSplit)split).getPath().getName();
			fileNameKey=new Text(path);
			
		}

		@Override
		protected void map(NullWritable key, BytesWritable value,
				Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		//	System.out.println(fileNameKey);
			context.write(fileNameKey, value);
		}
		
	}
}
