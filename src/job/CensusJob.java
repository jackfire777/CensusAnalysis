package job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CensusJob {
	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "CensusData");

			job.setJarByClass(CensusJob.class);
			job.setMapperClass(Mapper.class); // IdentityMapper
			job.setReducerClass(CensusReducer.class);
			job.setCombinerClass(CensusCombiner.class);
			job.setInputFormatClass(inputformat.CensusInputFormat.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(writables.CensusValueWritable.class);

			job.setNumReduceTasks(1);

			job.setOutputKeyClass(writables.CensusAnswers78.class);
			job.setOutputValueClass(NullWritable.class);

			MultipleOutputs.addNamedOutput(job, "Q1toQ6",
					TextOutputFormat.class,
					writables.CensusAnswers.class, NullWritable.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.waitForCompletion(true);
		} catch (IOException e) {
			System.err.println(e.getMessage());
		} catch (InterruptedException e) {
			System.err.println(e.getMessage());
		} catch (ClassNotFoundException e) {
			System.err.println(e.getMessage());
		}
	}
}
