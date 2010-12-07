package com.github.cablegate;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.analysis.snowball.SnowballAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import com.github.cablegate.input.WikimediaSimplifyInputFormat;
import com.github.cablegate.io.DocScore;
import com.github.cablegate.mapreduce.CategoryHighscoreReducer;
import com.github.cablegate.mapreduce.ScoreSumReducer;
import com.github.cablegate.mapreduce.TextDoubleSum;
import com.github.cablegate.mapreduce.WPCategories;
import com.github.cablegate.mapreduce.WPLuceneCategorizer;
import com.github.cablegate.mapreduce.WPLuceneMapper;

public class Tool extends Configured implements org.apache.hadoop.util.Tool {

    public static void main(String[] args) {
        int result = -1;
        try {
            result = ToolRunner.run(new Configuration(), new Tool(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(result);
    }

    public void createIndex() {
        try {
            MMapDirectory dir = new MMapDirectory(new File("index"));
            IndexWriter writer = new IndexWriter(
                dir,
                new SnowballAnalyzer(Version.LUCENE_30, "English"),
                true,
                MaxFieldLength.UNLIMITED
            );
            writer.setMaxBufferedDocs(1000);
            writer.setInfoStream(System.out);
            writer.setUseCompoundFile(true);
            File[] files = new File("cables").listFiles();
            for (File f: files) {
                if (!f.isFile() || !f.canRead() || f.isHidden()) {
                    continue;
                }
                System.out.println("INDEX " + f.toString());
                Document doc = new Document();
                Field filename = new Field("filename", f.toString(), Store.YES, Index.NO);
                InputStreamReader reader =
                    new InputStreamReader(new FileInputStream(f));
                Field content = new Field("content", reader);
                doc.add(filename);
                doc.add(content);
                writer.addDocument(doc);
            }
            System.out.println("OPTIMIZE");
            writer.optimize(1, true);
            writer.close();
        } catch (CorruptIndexException e) {
            e.printStackTrace();
        } catch (LockObtainFailedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        // uploading the assets
        FileSystem fileSystem = FileSystem.get(conf);
        if (!fileSystem.exists(new Path("/wikipedia"))) {
            fileSystem.mkdirs(new Path("/wikipedia"));
        }
        if (!fileSystem.exists(
                new Path("/wikipedia/enwiki-latest-pages-articles.xml.bz2"))
        ) {
            fileSystem.copyFromLocalFile(
                false,
                false,
                new Path("wikipedia/enwiki-latest-pages-articles.xml.bz2"),
                new Path("/wikipedia/enwiki-latest-pages-articles.xml.bz2")
            );
        }

        // normalize the categories
        if (!fileSystem.exists(
            new Path("/wikicategoriefrequnecy/part-r-00000"))
        ) {
            Job job = new Job(conf, "category analysis");
            job.setJarByClass(Tool.class);
            job.setInputFormatClass(WikimediaSimplifyInputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(DoubleWritable.class);
            job.setMapperClass(WPCategories.class);
            job.setReducerClass(TextDoubleSum.class);
            job.setCombinerClass(TextDoubleSum.class);
            FileInputFormat.setInputPaths(job, new Path("/wikipedia"));
            FileOutputFormat.setOutputPath(job, new Path("/wikicategoriefrequnecy"));
            job.submit();
            job.waitForCompletion(true);
        }

        // create / upload the index
        if (!fileSystem.exists(new Path("/index"))) {
            fileSystem.mkdirs(new Path("/index"));
            createIndex();
            for (File f: new File("index").listFiles()) {
                if (!f.canRead() || !f.isFile() || f.isHidden()) {
                    continue;
                }
                fileSystem.copyFromLocalFile(
                    false,
                    true,
                    new Path(f.toString()),
                    new Path("/" + f.toString())
                );
            }
        }

        ArrayList<URI> uriList = new ArrayList<URI>();
        uriList.add(new Path("/wikicategoriefrequnecy/part-r-00000").toUri());
        FileStatus[] listStatus = fileSystem.listStatus(new Path("/index"));
        for (FileStatus status: listStatus) {
            if (!status.isFile()) {
                continue;
            }
            uriList.add(status.getPath().toUri());
        }
        URI uris[] = new URI[uriList.size()];

        if (fileSystem.exists(new Path("/wikicategories"))) {
            fileSystem.delete(new Path("/wikicategories"), true);
        }

        Job job = new Job(conf, "cable analysis");
        job.setCacheFiles(uriList.toArray(uris));
        uriList = null;
        job.setJarByClass(Tool.class);
        job.setInputFormatClass(WikimediaSimplifyInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DocScore.class);
        job.setMapperClass(WPLuceneCategorizer.class);
        job.setReducerClass(CategoryHighscoreReducer.class);
        job.setCombinerClass(ScoreSumReducer.class);
        FileInputFormat.setInputPaths(job, new Path("/wikipedia"));
        FileOutputFormat.setOutputPath(job, new Path("/wikicategories"));
        job.submit();
        job.waitForCompletion(true);
        return 0;
    }

}
