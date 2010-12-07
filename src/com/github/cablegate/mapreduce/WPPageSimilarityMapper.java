package com.github.cablegate.mapreduce;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;

import com.github.cablegate.io.DocScore;

public class WPPageSimilarityMapper extends
    WPLuceneMapper<LongWritable, Text, Text, DocScore>
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

        if (wiki.indexOf("</title>") == -1) {
            return;
        }

        String title = wiki.substring(
            wiki.indexOf("<title>") + 1, wiki.indexOf("</title>")
        );

        wiki = wiki.substring(
            wiki.indexOf("<text xml:space=\"preserve\">") + 27
        );
        wiki = wiki.substring(0, wiki.indexOf("</text>"));

        if (wiki.length() < 2048) {
            return;
        }

        // Step 2 - get text tokens
        Query like = likeThis.like(new StringReader(wiki));

        // Step 3 - search
        TopDocs docs = searcher.search(like, 20);

        if (docs.totalHits == 0) {
            return;
        }

        DocScore ds = new DocScore();
        Text t = new Text(title);

        // Step 4 - emit results
        for (int i = 0; i < docs.scoreDocs.length; i++) {
            Document doc = searcher.doc(docs.scoreDocs[i].doc);
            ds.setDocument(doc.getField("filename").stringValue());
            ds.setScore(docs.scoreDocs[i].score);
            context.write(t, ds);
        }
    }

}
