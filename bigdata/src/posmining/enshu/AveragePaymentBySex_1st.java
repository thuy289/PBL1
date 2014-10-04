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

public class AveragePaymentBySex_1st {


	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		Job job = new Job(new Configuration());
		job.setJarByClass(AveragePaymentBySex_1st.class);
		job.setMapperClass(OnigiriMapper.class);
		job.setReducerClass(OnigiriReducer.class);
		job.setJobName("2014012");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(CSKV.class);
		job.setMapOutputValueClass(CSKV.class);
		job.setOutputKeyClass(CSKV.class);
		job.setOutputValueClass(CSKV.class);

		String inputpath = "posdata";
		String outputpath = "out/averagePayment1st";

		if (args.length > 0) {
			inputpath = args[0];
		}

		FileInputFormat.setInputPaths(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));

		PosUtils.deleteOutputDir(outputpath);

		job.setNumReduceTasks(8);

		job.waitForCompletion(true);

	}

	public static class OnigiriMapper extends Mapper<LongWritable, Text, CSKV, CSKV> {

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String csv[] = value.toString().split(",");

			String reciptID = csv[PosUtils.RECEIPT_ID];

			String sex = csv[PosUtils.BUYER_SEX];
			String price = csv[PosUtils.ITEM_TOTAL_PRICE];

			String val = sex + "," + price;

			context.write(new CSKV(reciptID), new CSKV(val));
		}

	}

	public static class OnigiriReducer extends Reducer<CSKV, CSKV, CSKV, CSKV> {

		protected void reduce(CSKV key, Iterable<CSKV> values, Context context) throws IOException, InterruptedException {

			int totalPrice = 0;
			String sex = null;
			int price;
			for (CSKV value : values) {
				String temp = value.toString();
				String[] tempArray = temp.split(",");
				sex = tempArray[0].toString();

				try {
					price = Integer.parseInt(tempArray[1].toString());
					totalPrice += price;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			context.write(new CSKV(sex), new CSKV(totalPrice));
		}
	}

}
