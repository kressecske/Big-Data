package hu.elte.bigdata;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Big Data Feladat1");
		job.setJarByClass(hu.elte.bigdata.WordCountDriver.class);
		job.setMapperClass(hu.elte.bigdata.WordCountMapper.class);

		job.setReducerClass(hu.elte.bigdata.WordCountReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BooleanWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("test.txt"));
		FileOutputFormat.setOutputPath(job, new Path("feladat1Out2"));

		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			if (fs.exists(new Path("feladat1Out")))
				fs.delete(new Path("feladat1Out"),true);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		if (!job.waitForCompletion(true))
			return;
	}

}
