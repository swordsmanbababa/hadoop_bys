package train;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import prepare.CountDocType;

import tools.Global_Env;

public class Judge {
	static HashMap<String,String>  classOfDoc=new HashMap<String, String>();
	static HashMap<String,String>  resMap=new HashMap<String, String>();
	static HashSet<String> classCSet=new HashSet<String>();
	
	static  HashMap<String,Double> precisionMap=new HashMap<String,Double>();
	static HashMap<String,Double> recallMap=new HashMap<String,Double>();
	static HashMap<String,Double> f1Map=new HashMap<String,Double>();
	
	public static void main(String[] args) {
		readClassOfDoc();
		readResult();
		evaluationForC();
		macroPrecision();
		//microPrecision();
		//totalR();
	}
	
	//读取文件的真实类型
	static void readClassOfDoc(){
	   Configuration conf = new Configuration();
       String path = "docCountOutput/part-r-00000";
       FileSystem fs;
		try {
			fs = FileSystem.get(URI.create(path),conf);
			InputStream in=null;
			in=fs.open(new Path(path));
			BufferedReader br=new BufferedReader(new InputStreamReader(in));
			String line=null;
			while((line=br.readLine())!=null){
				//System.out.println("readClassOfDoc ----------"+line);
				String[] split=line.split("\\s+");
				String classC=split[0];
				String doc=split[1];
				classOfDoc.put(doc,classC);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	//读取文件的预测类型
	static void readResult(){
		Configuration conf = new Configuration();
	       String path = Global_Env.testResultPath+"/part-r-00000";
	       FileSystem fs;
			try {
				fs = FileSystem.get(URI.create(path),conf);
				InputStream in=null;
				in=fs.open(new Path(path));
				BufferedReader br=new BufferedReader(new InputStreamReader(in));
				String line=null;
				while((line=br.readLine())!=null){
				//	System.out.println("readResult ----------"+line);
					String[] split=line.split("\\s+");
					String classC=split[1];
					String doc=split[0];
					resMap.put(doc,classC);
					classCSet.add(classC);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	
	static void evaluationForC(){
		Iterator<String> iterator = classCSet.iterator();
		while(iterator.hasNext()){
			 double TP=0;
			 double TN=0;
			 double FP=0;
			 double FN=0;
			 String c=iterator.next();
			 for(Map.Entry<String, String> testedDoc:resMap.entrySet()){
				 String docName=testedDoc.getKey();
				 String resClass=testedDoc.getValue();
				 String oriClass=classOfDoc.get(docName);
				 
				 if(c.equals(oriClass)&&c.equals(resClass))
					 TP++;
				 else if(c.equals(oriClass)&&!c.equals(resClass))
					 FN++;
				 else if(!c.equals(oriClass)&&c.equals(resClass))
					 FP++;
				 else if(!c.equals(oriClass)&&!c.equals(resClass))
					 TN++;
				// System.out.println(c+"docName="+docName+"  oriClass="+oriClass+"   resClass="+resClass+"---TP="+TP+"  TN="+TN+"  FP="+FP+"  FN="+FN+"  f1="+f1Map.get(c));
			 }
			double precision=TP/(TP+FP);
			double recall=TP/(TP+FN);
			double f1=2*precision*recall/(precision+recall);
			if(TP!=0)
				f1Map.put(c, f1);
			else
				f1Map.put(c, 0.0);
			System.out.println(c+"---TP="+TP+"  TN="+TN+"  FP="+FP+"  FN="+FN+" precision="+precision+"  recall"+recall+"   f1="+f1Map.get(c));
		 }
	}
	
	static void macroPrecision(){
		double precision=0;
		for(Map.Entry<String, Double> entry:f1Map.entrySet()){
			precision+=entry.getValue();
		}
		precision/=f1Map.size();
		System.out.println("macroPrecision---------  "+precision*100+"%");
	}
	
	
	static void microPrecision(){
		 double TP=0;
		 double TN=0;
		 double FP=0;
		 double FN=0;
		Iterator<String> iterator = classCSet.iterator();
		while(iterator.hasNext()){
			 String c=iterator.next();
			 for(Map.Entry<String, String> testedDoc:resMap.entrySet()){
				 String docName=testedDoc.getKey();
				 String resClass=testedDoc.getValue();
				 String oriClass=classOfDoc.get(docName);
				 
				 if(c.equals(oriClass)&&c.equals(resClass))
					 TP++;
				 else if(c.equals(oriClass)&&!c.equals(resClass))
					 FN++;
				 else if(!c.equals(oriClass)&&c.equals(resClass))
					 FP++;
				 else if(!c.equals(oriClass)&&!c.equals(resClass))
					 TN++;
			 }
		 }
		double precision=TP/(TP+FP);
		double recall=TP/(TP+FN);
		
		double f1=2*precision*recall/(precision+recall);
		System.out.println("microPrecision---------  "+precision*100+"%");
	}
	static void totalR(){
		int totalCount=0;
		int rightCount=0;
		System.out.println("total-------"+resMap.size());
		for(Map.Entry<String,String> entry:resMap.entrySet()){
			totalCount++;
			String doc=entry.getKey();
			String classC=entry.getValue();
			
			String oriClass=classOfDoc.get(doc);
			System.out.println("total ----------"+classC+"  "+oriClass);
			if(classC.equals(oriClass)){
				rightCount++;
			}
		}
		System.out.println(totalCount+ "  "+rightCount);
	}
}
