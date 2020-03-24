package prepare;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;

public class DocNumRecordReader extends RecordReader<Text,IntWritable>{
	private FileSplit fileSplit;
	private Configuration conf;
	private Text key=new Text();
	private IntWritable value=new IntWritable(1);
	private boolean processed=false;
	SequenceFile.Reader reader=null;
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		 IOUtils.closeStream(reader);
	}

	
	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return processed ?1.0f:0.0f;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	//	System.out.println("record_reader initial");
		this.fileSplit=(FileSplit)split;
		this.conf=context.getConfiguration();
		this.reader = new SequenceFile.Reader(fileSplit.getPath().getFileSystem(conf), fileSplit.getPath(), conf);
	}

//	@Override
//	public boolean nextKeyValue() throws IOException, InterruptedException {
//		// TODO Auto-generated method stub\
//		//System.out.println("record_reader: porcessed="+processed);
//		Path file=fileSplit.getPath();
//		if(!processed){	
//		String fileName=file.toString();
//		key.set(fileName);
//		value=(new IntWritable(1));
//	//System.out.println("record_reader:key-value: "+key+"_"+value);
//		processed=true;
//		return true;
//		}
//		
//		return false;
//	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub\
	
		if(!processed){
		readSeqSetKV();
	
		}
		//System.out.println("record_reader: porcessed= "+processed+" \n");
		return !processed;
	}


	private void readSeqSetKV() {
		// TODO Auto-generated method stub
		 try {
			
			Writable key_s = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value_s = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			
			
			boolean unFinished=true;
			boolean keyNotEmpty=false;
			while(!keyNotEmpty&&unFinished){
				unFinished=reader.next(key_s, value_s);
			//	System.out.println("key_s"+key_s.toString().trim());
				if(!key_s.toString().trim().equals(""))
					keyNotEmpty=true;
			}
		//	System.out.println("key: "+key_s+"\n");
		   
			
		   
		    if(!unFinished){
            	processed=true;
	         }
		    
		    if(keyNotEmpty==true){
		    String text=(new Path(key_s.toString())).getParent().toString()+"  "+(new Path(key_s.toString()).getName()).toString();
		    key.set(new Text(text));
      //  	key.set(key_s.toString());
        	value=(new IntWritable(1));
		    }
        	
        //    System.out.printf("%s\t%s\n", key_s, value_s);
           
          
          
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
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

}
