package com.github.cablegate.mapreduce;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;

import com.github.cablegate.io.DocScore;

public class WPLuceneCategorizer extends
    WPLuceneMapper<LongWritable, Text, Text, DocScore>
{

    private HashMap<String, Double> categoryScore;

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

        // Step 3 - get text tokens
        Query like = likeThis.like(new StringReader(wiki));

        // Step 4 - search
        TopDocs docs = searcher.search(like, 20);

        if (docs.totalHits == 0) {
            return;
        }

        DocScore ds = new DocScore();
        Text t = new Text();

        // Step 5 - emit results
        for (int i = 0; i < docs.scoreDocs.length; i++) {
            Document doc = searcher.doc(docs.scoreDocs[i].doc);
            t.set(doc.getField("filename").stringValue());
            for (String cat: categories) {
                ds.setDocument(cat);
                ds.setScore(docs.scoreDocs[i].score / categories.length
                        / categoryScore.get(cat));
                context.write(t, ds);
            }
        }
    }

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        super.setup(context);
        // scan for the part-r file with category scorings....
        categoryScore = new HashMap<String, Double>();
        Path p[] = context.getLocalCacheFiles();
        for (Path path: p) {
            if (path.toString().endsWith("/part-r-00000")) {
                LineNumberReader lnr = new LineNumberReader(
                    new InputStreamReader(new FileInputStream(path.toString()))
                );
                String line = lnr.readLine();
                while (line != null) {
                    String l[] = line.split("\t");
                    categoryScore.put(l[0], Double.parseDouble(l[1]));
                    line = lnr.readLine();
                }
                lnr.close();
            }
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        super.cleanup(context);
        categoryScore = null;
    }

}
