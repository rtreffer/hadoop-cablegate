package com.github.cablegate.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TextDoubleSum extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double result = 0d;
        for (DoubleWritable value: values) {
            result += value.get();
        }
        context.write(key, new DoubleWritable(result));
    }

}
