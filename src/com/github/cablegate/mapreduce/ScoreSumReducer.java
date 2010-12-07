package com.github.cablegate.mapreduce;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.github.cablegate.io.DocScore;

public class ScoreSumReducer extends Reducer<Text, DocScore, Text, DocScore>{

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
        DocScore ds = new DocScore();
        for (String cat: scoreByCategorie.keySet()) {
            ds.setDocument(cat);
            ds.setScore(scoreByCategorie.get(cat));
            context.write(key, ds);
        }
    }

}
