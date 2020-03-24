package prepare;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;

public class DocTypeRecordReader extends RecordReader<Text, IntWritable>{
	private IntWritable value=new IntWritable(0);
	private Text key=new Text();
	private FileSplit fileSplit;
	private Configuration conf;
	private boolean processed=false;
	SequenceFile.Reader reader=null;
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public IntWritable getCurrentValue() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return processed?1.0f:0.0f;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		this.fileSplit=(FileSplit)split;
		this.conf=context.getConfiguration();
		this.reader = new SequenceFile.Reader(fileSplit.getPath().getFileSystem(conf), fileSplit.getPath(), conf);
	}

//	@Override
//	public boolean nextKeyValue() throws IOException, InterruptedException {
//		// TODO Auto-generated method stub
//		if(!processed){
//			//System.out.println(""+fileSplit.getPath());
//			String fileDir=fileSplit.getPath().getParent().getName();
//			key.set(fileDir);
//			value=new IntWritable(1);
//			processed=true;
//			return true;
//		}
//		return false;
//	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(!processed){
			Writable key_s = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value_s = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);		
			boolean unFinished=reader.next(key_s, value_s);
			
			if(key_s.toString().trim().equals("")){
				processed=true;
				return true;
			}			
			//返回文档父路径即类名		
			key.set(new Text((new Path(key_s.toString())).getParent().toString()));
//			byte[] fliebites =((BytesWritable)value_s).getBytes();
//			String result = new String(fliebites);
//			System.out.println("fileValue : "+result);
			value=new IntWritable(1);
			if(!unFinished){
				processed=true;
			}
		}
		return !processed;
	}

}
