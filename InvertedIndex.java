import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedIndex {

	public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text>{
		private Text keyInfo = new Text();  //存储单词和URI的组合
		private Text valueInfo = new Text();//存储词频
		private FileSplit split;            //存储Split对象
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			split = (FileSplit)context.getInputSplit();//获得<key,value>对所属的FileSplit对象
			StringTokenizer itr = new StringTokenizer(value.toString());
			while(itr.hasMoreTokens()){
				//key值由单词和URI组成，如“MapReduce:1.txt”
				//keyInfo.set(itr.nextToken() + ":" + split.getPath().toString());
				keyInfo.set(itr.nextToken() + ":" + split.getPath().getName());
				
				//The file containing this split's data. 
				//System.out.println(split.getPath().toString());//得到的是包含该split的文件路径
				//System.out.println(split.getPath().getName());//得到的是包含该split的文件名
				
				//词频初始为1
				valueInfo.set("1");
				context.write(keyInfo, valueInfo);
				System.out.println("key=" + keyInfo + "\t" + "value=" + valueInfo);
			}
		}
	}
	
	public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text>{
		private Text info = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			int sum = 0;// 统计词频
			for(Text value:values){
				sum += Integer.parseInt(value.toString());
			}
			
			int splitIndex = key.toString().indexOf(":");
			//重新设置value值由RUI和词频组成
			info.set(key.toString().substring(splitIndex + 1) + ":" + sum);
			//重新设置key值为单词
			key.set(key.toString().substring(0, splitIndex));
			context.write(key, info);
		}
	}
	
	public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>{
		private Text result = new Text();
		
		public void reduce(Text key, Iterable<Text>values, Context context) throws IOException, InterruptedException{
			//生成文档列表
			String fileList = new String();
			for(Text value:values){
				fileList += value.toString() + ";";
			}
			result.set(fileList);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if(otherArgs.length != 2){
			System.out.println("Usage:invertedIndex<in><out>");
			System.exit(2);
		}
		
		 /*判断输出目录文件是否存在，如果存在则先删除它*/
		FileSystem hdfs = FileSystem.get(conf);
		Path path = new Path(otherArgs[1]);
		if(hdfs.exists(path))
			hdfs.delete(path, true);//不递归删除
		
		Job job = new Job(conf, "invertedIndex");
		job.setJarByClass(InvertedIndex.class);
		
		job.setMapperClass(InvertedIndexMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setCombinerClass(InvertedIndexCombiner.class);
		job.setReducerClass(InvertedIndexReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
