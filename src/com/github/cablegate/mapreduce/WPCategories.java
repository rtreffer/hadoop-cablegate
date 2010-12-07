package com.github.cablegate.mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WPCategories extends
    Mapper<LongWritable, Text, Text, DoubleWritable>
{

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        if (value == null)  {
            return;
        }

        // Step 1 - title / text

        String wiki = value.toString();

        if (wiki.indexOf("<redirect />") != -1) {
            return;
        }

        if (wiki.indexOf("</text>") == -1) {
            return;
        }

        wiki = wiki.substring(
            wiki.indexOf("<text xml:space=\"preserve\">") + 27
        );
        wiki = wiki.substring(0, wiki.indexOf("</text>"));

        if (wiki.length() < 2048) {
            return;
        }

        // Step 2 - Categories
        String categories[] = Utils.extractCategories(wiki);

        if (categories.length == 0) {
            return;
        }

        // Step 3 - emit results

        Text t = new Text();
        DoubleWritable d = new DoubleWritable();

        d.set(1d / categories.length);
        for (String cat: categories) {
            t.set(cat);
            context.write(t, d);
        }
    }

    public String[] getCategories(String text) {
        ArrayList<String> categories = new ArrayList<String>();
        int pos = text.indexOf("[[Category:");
        while (pos != -1) {
            text = text.substring(pos + 11);
            int end = text.indexOf("]]");
            if (end == -1) {
                break;
            }
            int end2 = text.indexOf("|");
            if (end2 != -1) {
                end = Math.min(end, end2);
            }
            categories.add(text.substring(0, end));
            pos = text.indexOf("[[Category:");
        }
        if (categories.size() == 0) {
            return new String[0];
        }
        String result[] = new String[categories.size()];
        return categories.toArray(result);
    }

}
