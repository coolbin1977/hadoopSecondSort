package hadoopSecondSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import hadoopSecondSort.HadoopSecondSortKey.HadoopSecondSortKeyComparator;

public class HadoopSecondSortDriver extends Configured implements Tool {

	
	private static final int ONE_MB = (int) (1024 * 1024L);
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 2){
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		Configuration conf = new Configuration(getConf());
		conf.setInt("mapreduce.input.fileinputformat.split.maxsize", ONE_MB * 128);		
		Job job = Job.getInstance(conf,"HadoopSecondSort");		
		//job.setJarByClass(getClass());
		
		Path outputPath = new Path("hdfs://"+args[0]+":9000/"+"output/"+"HadoopSecondSort/");		
		Path inputPath = new Path("hdfs://"+args[0]+":9000/"+args[1]+"/");
		
		FileSystem fs = outputPath.getFileSystem(conf);		
		fs.delete(outputPath,true);
		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setJarByClass(HadoopSecondSortDriver.class);
        job.setJarByClass(HadoopSecondSortMap.class);
        job.setJarByClass(HadoopSecondSortReducer.class);		
		
		job.setMapperClass(HadoopSecondSortMap.class);
		job.setReducerClass(HadoopSecondSortReducer.class);
		
		job.setNumReduceTasks(5);
		
		job.setMapOutputKeyClass(HadoopSecondSortKey.class);
		job.setMapOutputValueClass(HadoopSecondSortKey.class);
		
		//job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(Text.class);	
		
		job.setPartitionerClass(hadoopSecondSortPartitioner.class);
		job.setGroupingComparatorClass(hadoopSecondSortGroupingComparator.class);
		job.setSortComparatorClass(HadoopSecondSortKeyComparator.class);
		
		//job.setInputFormatClass(TextInputFormat.class);
	    //job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(CombineTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
		
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new HadoopSecondSortDriver(), args);
		System.exit(exitCode);
	}

}
