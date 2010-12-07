package com.github.cablegate.mapreduce;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.snowball.SnowballAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similar.MoreLikeThis;
import org.apache.lucene.util.Version;

import com.github.cablegate.DistributedCacheDirectory;

public class WPLuceneMapper<IK, IV, OK, OV> extends Mapper<IK, IV, OK, OV>{

    protected IndexSearcher searcher;
    protected MoreLikeThis likeThis;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        super.setup(context);
        Path p[] = context.getLocalCacheFiles();
        for (Path path: p) {
            System.err.println(path.toString());
        }
        DistributedCacheDirectory directory = new DistributedCacheDirectory(p);
        searcher = new IndexSearcher(directory, true);
        likeThis = new MoreLikeThis(IndexReader.open(directory, true));
        likeThis.setAnalyzer(
            new SnowballAnalyzer(Version.LUCENE_30, "English")
        );
        likeThis.setFieldNames(new String[]{"content"});
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        super.cleanup(context);
        searcher.close();
    }

}
