package com.adintellig.advindex;

// /data/dm/baofengindex/crowd_attrs/bf_index_vv_play_album_crowd_attrs.txt

// /data/dm/advindex/adv_hour_movie/
///data/dm/advindex/adv_hour_movie/20130101/00

// /data/dm/advindex/material_hour_movie
// /data/dm/advindex/material_hour_movie/20130101/00

import com.adintellig.util.DateFormatUtil;
import java.io.File;
import java.io.IOException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Main {
	static final Log LOG = LogFactory.getLog(Main.class);

	public static final String NAME = "Crowd-Attribute";
	public static final String TMP_FILE_PATH = "/tmp/advindex";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		CommandLine cmd = parseArgs(otherArgs);
		String output = cmd.getOptionValue("o");
		String input = cmd.getOptionValue("i");
		String date = cmd.getOptionValue("d");
		String hour = cmd.getOptionValue("h");
		String jobConf = cmd.getOptionValue("jobconf");
		String[] arr = null;
		if (null != jobConf && jobConf.length() > 0) {
			arr = jobConf.split("=", -1);
			if (arr.length == 2) {
				conf.set(arr[0], arr[1]);
			}
		} else {
			conf.set("mapred.job.queue.name", "ETL");
		}

		if (null == date || date.length() <= 0) {
			date = DateFormatUtil.parseToStringDate(System.currentTimeMillis());
		}

		if (null == hour || hour.length() <= 0) {
			hour = DateFormatUtil.getLastHour();
		}

		if (output.endsWith(File.separator)) {
			output = output + date;
		} else
			output = output + File.separator + date;

		FileSystem fs = FileSystem.get(conf);
		Path outputPath = new Path(output);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		Path tmpPath = new Path(TMP_FILE_PATH);
		if (fs.exists(tmpPath)) {
			fs.delete(tmpPath, true);
		}

		// generate hour input path
		arr = input.split(",", -1);
		if (arr.length == 2) {
			String hourData = null;
			String attrData = null;
			if (arr[0].indexOf("advindex") >= 0) {
				hourData = arr[0].trim();
				attrData = arr[1].trim();
			} else {
				hourData = arr[1].trim();
				attrData = arr[0].trim();
			}

			String hourInput = generateHourDataInput(hourData, date, hour);
			if (hour.equals("0") || hour.equals("00")) {
				date = DateFormatUtil.parseToStringDate2(DateFormatUtil
						.formatStringTimeToLong2(date) - 3600 * 60 * 24);
				hourInput = generateHourDataInput(hourData, date, "24");
			}
			input = hourInput + "," + attrData;
		}

		System.out.println("Inputs: " + input);
		System.out.println("Output: " + output);
		System.out.println("TempOut: " + TMP_FILE_PATH);

		/* step1: adid-attr mapping */
		Job job = new Job(conf, "advindex adid-attr mapping");
		job.setJarByClass(Main.class);
		job.setMapperClass(AdidAttrMapper.class);
		job.setReducerClass(AdidAttrReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, tmpPath);

		int success = job.waitForCompletion(true) ? 0 : 1;

		if (success == 0) {
			/* step2: aggregation */
			job = new Job(conf, "advindex aggregation");
			job.setJarByClass(Main.class);
			job.setMapperClass(AggrMapper.class);
			job.setReducerClass(AggrReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.setInputPaths(job, tmpPath);
			FileOutputFormat.setOutputPath(job, outputPath);

			success = job.waitForCompletion(true) ? 0 : 1;
		}

		// delete tmp dirctory
		if (fs.exists(tmpPath)) {
			fs.delete(tmpPath, true);
		}

	}

	private static String generateHourDataInput(String input, String date,
			String hour) throws IOException {
		StringBuilder sb = new StringBuilder();
		if (null != input && null != date && null != hour) {
			if (input.endsWith(File.separator)) {
				input = input.substring(0, input.length() - 1);
			}

			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] fstat = fs.listStatus(new Path(input, date));
			int maxHourNum = Integer.parseInt(hour);
			for (FileStatus stat : fstat) {
				String fileName = stat.getPath().getName();
				int hourNum = Integer.parseInt(fileName);
				if (hourNum < maxHourNum) {
					sb.append(stat.getPath() + ",");
				}
			}

			if (sb.toString().length() > 0)
				sb.setLength(sb.toString().length() - 1);

		}
		return sb.toString();
	}

	private static CommandLine parseArgs(String[] args) throws ParseException {
		Options options = new Options();
		Option o = new Option("i", "input", true,
				"the directory or file to read from (must exist)");
		o.setArgName("input");
		o.setRequired(true);
		options.addOption(o);

		o = new Option("o", "output", true, "output directory (must exist)");
		o.setArgName("output");
		o.setRequired(true);
		options.addOption(o);

		o = new Option("d", "date", true,
				"the start date of data, such as: 20130101");
		o.setArgName("date");
		o.setRequired(false);
		options.addOption(o);

		o = new Option("h", "hour", true, "hour, such as 01");
		o.setArgName("hour");
		o.setRequired(false);
		options.addOption(o);

		o = new Option("jobconf", "hour", true, "jobconf");
		o.setArgName("jobconf");
		o.setRequired(false);
		options.addOption(o);

		o = new Option("file", "hour", true, "file");
		o.setArgName("file");
		o.setRequired(false);
		options.addOption(o);

		o = new Option("mapred_output_dir", "hour", true, "file");
		o.setArgName("mapred_output_dir");
		o.setRequired(false);
		options.addOption(o);

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (Exception e) {
			System.err.println("ERROR: " + e.getMessage() + "\n");
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(NAME + " ", options, true);
			System.exit(-1);
		}
		return cmd;
	}

}
