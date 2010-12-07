package com.github.cablegate.input;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Simple input format to enforce a WikimediaSimplifyRecordReader.
 */
public class WikimediaSimplifyInputFormat extends MatchInputFormat {

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
	TaskAttemptContext context) throws IOException, InterruptedException {
		return new WikimediaSimplifyRecordReader();
	}


}
