package posmining.enshu;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import posmining.utils.CSKV;
import posmining.utils.PosUtils;

public class AveragePaymentBySex_2nd {


	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		Job job = new Job(new Configuration());
		job.setJarByClass(AveragePaymentBySex_2nd.class);
		job.setMapperClass(OnigiriMapper.class);
		job.setReducerClass(OnigiriReducer.class);
		job.setJobName("2014012");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(CSKV.class);
		job.setMapOutputValueClass(CSKV.class);
		job.setOutputKeyClass(CSKV.class);
		job.setOutputValueClass(CSKV.class);

		String inputpath = "out/averagePayment1st";
		String outputpath = "out/averagePaymentFinal";

		FileInputFormat.setInputPaths(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));

		if (args.length > 0) {
			inputpath = args[0];
		}

		PosUtils.deleteOutputDir(outputpath);

		job.setNumReduceTasks(8);

		job.waitForCompletion(true);

	}

	public static class OnigiriMapper extends Mapper<LongWritable, Text, CSKV, CSKV> {

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String csv[] = value.toString().split("\t");
			context.write(new CSKV(csv[0]), new CSKV(csv[1]));
		}

	}

	public static class OnigiriReducer extends Reducer<CSKV, CSKV, CSKV, CSKV> {

		protected void reduce(CSKV key, Iterable<CSKV> values, Context context) throws IOException, InterruptedException {

			long avgPrice = 0;
			int count = 0;
			for (CSKV value : values) {
				count ++;
				avgPrice += value.toInt();
			}

			avgPrice /= count;
			context.write(key, new CSKV(avgPrice));
		}
	}

}
