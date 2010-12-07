package com.github.cablegate.mapreduce;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.github.cablegate.io.DocScore;

public class CategoryHighscoreReducer extends
        Reducer<Text, DocScore, Text, Text> {

    private final static DocComparator comparator = new DocComparator();

    @Override
    protected void reduce(Text key, Iterable<DocScore> values, Context context)
        throws IOException, InterruptedException
    {
        HashMap<String, Double> scoreByCategorie =
            new HashMap<String, Double>();
        for (DocScore d: values) {
            String cat = d.getDocument();
            if (scoreByCategorie.containsKey(cat)) {
                scoreByCategorie.put(
                    cat, d.getScore() + scoreByCategorie.get(cat)
                );
            } else {
                scoreByCategorie.put(cat, d.getScore());
            }
        }
        TreeSet<DocScore> highscore = new TreeSet<DocScore>(comparator);
        for (String cat: scoreByCategorie.keySet()) {
            DocScore ds = new DocScore(cat, scoreByCategorie.get(cat));
            highscore.add(ds);
            if (highscore.size() > 10) {
                highscore.remove(highscore.last());
            }
        }
        StringBuilder output = new StringBuilder();
        for (DocScore ds: highscore) {
            output.append(ds.getDocument());
            output.append(" (");
            output.append(ds.getScore());
            output.append(") / ");
        }
        output.setLength(output.length() - 3);
        context.write(key, new Text(output.toString()));
    }

    private static class DocComparator implements Comparator<DocScore> {

        @Override
        public int compare(DocScore o1, DocScore o2) {
            if (o1.getScore() > o2.getScore()) {
                return -1;
            }
            if (o1.getScore() < o2.getScore()) {
                return 1;
            }
            return o1.getDocument().compareTo(o2.getDocument());
        }

    }

}
