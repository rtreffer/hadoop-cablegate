package com.github.cablegate.input;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * MatchInputFormat is a {@link MatchRecordReader} only input format.
 */
public class MatchInputFormat extends FileInputFormat<LongWritable, Text> {

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
	TaskAttemptContext context) throws IOException, InterruptedException {
		return new MatchRecordReader();
	}

	@Override
	protected boolean isSplitable(JobContext job, Path path) {
		final CompressionCodec codec = new CompressionCodecFactory(
		job.getConfiguration()).getCodec(path);
		return (codec == null) || (codec instanceof SplittableCompressionCodec);
	}

}
