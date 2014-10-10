package posmining.pbl1.sake;

import java.io.IOException;
import java.util.ArrayList;

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

public class CountItemNumBoughtWithSake {

	private static final String JOBNAME = "2014003";
	private static final String INPUTPATH = "posdata";
	private static final String OUTPUTPATH = "out/CountItemNumBoughtWithSake";

	private static final Class<CountItemNumBoughtWithSake> JARCLASS = CountItemNumBoughtWithSake.class;
	private static final Class<CountItemNumBoughtWithSakeMapper> MAPPERCLASS = CountItemNumBoughtWithSakeMapper.class;
	private static final Class<CountItemNumBoughtWithSakeReducer> REDUCERCLASS = CountItemNumBoughtWithSakeReducer.class;

	private static final String[] sakeList = { "ビール", "酒", "チューハイ", "ワイン",
			"ウイスキー", "カクテル" };

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		Job job = new Job(new Configuration());
		job.setJarByClass(JARCLASS);

		// 入出力形式の指定
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// アウトプットのキーバリューの型をそれぞれ設定
		job.setMapOutputKeyClass(CSKV.class);
		job.setMapOutputValueClass(CSKV.class);
		job.setOutputKeyClass(CSKV.class);
		job.setOutputValueClass(CSKV.class);

		// インプットファイルのパスを指定
		if (args.length > 0) {
			FileInputFormat.setInputPaths(job, new Path(args[0]));
		} else {
			FileInputFormat.setInputPaths(job, new Path(INPUTPATH));
		}
		FileOutputFormat.setOutputPath(job, new Path(OUTPUTPATH));
		// いったんアウトプットパスを削除して，ロック状態を解放しておく
		PosUtils.deleteOutputDir(OUTPUTPATH);

		// タスク数の設定
		job.setNumReduceTasks(8);

		// MapperとReducerの設定
		job.setJobName(JOBNAME);
		job.setMapperClass(MAPPERCLASS);
		job.setReducerClass(REDUCERCLASS);

		job.waitForCompletion(true);

	}

	public static class CountItemNumBoughtWithSakeMapper extends
			Mapper<LongWritable, Text, CSKV, CSKV> {

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String csv[] = value.toString().split(",");

			String reciptID = csv[PosUtils.RECEIPT_ID];
			String itemName = csv[PosUtils.ITEM_NAME];
			String itemCount = csv[PosUtils.ITEM_COUNT];
			String itemCategoryName = csv[PosUtils.ITEM_CATEGORY_NAME];

			String val = itemName + "," + itemCount + "," + itemCategoryName;

			context.write(new CSKV(reciptID), new CSKV(val));
		}

	}

	public static class CountItemNumBoughtWithSakeReducer extends
			Reducer<CSKV, CSKV, CSKV, CSKV> {

		protected void reduce(CSKV key, Iterable<CSKV> values, Context context)
				throws IOException, InterruptedException {

			Boolean hasSake = false;

			ArrayList<String> valuesCopy = new ArrayList<String>();

			for (CSKV value : values) {
				valuesCopy.add(new String(value.toString()));
			}

			label: for (String valueStr : valuesCopy) {
				String itemCategoryName = valueStr.split(",")[2];

				for (String sake : sakeList) {
					if (itemCategoryName.matches(".*" + sake + ".*")) {
						hasSake = true;
						break label;
					}
				}
			}

			if (hasSake) {
				for (String valueStr : valuesCopy) {
					context.write(key, new CSKV(valueStr));
				}
			}
		}
	}

}
