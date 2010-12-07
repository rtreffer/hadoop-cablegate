package com.github.cablegate.input;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * A {@link RecordReader} for records that start/end with a fixed string.
 */
public class MatchRecordReader extends RecordReader<LongWritable, Text> {

    private long splitStart;
    private long splitEnd;
    private long lastStartPos;
    private long lastStartId;
    private long maxRecordSize;
    protected byte[] startSequence;
    protected byte[] endSequence;
    private boolean closed;
    private Seekable fileIn;
    private InputStream dataIn;
    private LongWritable currentKey;
    private Text currentValue;

    protected ByteArrayOutputStream valueBuffer;
    protected OutputStream out;
    private Counter inputByteCounter;
    private MapContext<?, ?, ?, ?> context;

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        currentKey = null;
        currentValue = null;
        endRecord();
        dataIn.close();
        fileIn = null;
        dataIn = null;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException,
            InterruptedException {
        return currentKey;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (closed) {
            return 1f;
        }
        if (fileIn == null) {
            return 0f;
        }
        long splitPos = fileIn.getPos();
        float progress = ((float) (splitPos - splitStart))
                / ((float) (splitEnd - splitStart));
        return Math.min(0.99f, progress);
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext ctx)
            throws IOException, InterruptedException {

        this.context = ((MapContext<?, ?, ?, ?>) ctx);
        context.setStatus("Running");
        inputByteCounter = context.getCounter(FileInputFormat.COUNTER_GROUP,
                FileInputFormat.BYTES_READ);

        currentKey = new LongWritable();
        currentValue = new Text();
        FileSplit split = (FileSplit) genericSplit;
        final Path file = split.getPath();
        final Configuration conf = context.getConfiguration();
        startSequence = conf.get("mapred.matchreader.record.start", "")
                .getBytes();
        endSequence = conf.get("mapred.matchreader.record.end", "").getBytes();
        maxRecordSize = conf.getLong("mapred.matchreader.record.maxSize",
                Long.MAX_VALUE);
        final CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(
                conf);
        final FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream fileIn = fs.open(split.getPath());
        final CompressionCodec codec = compressionCodecs.getCodec(file);
        if (codec == null) {
            splitEnd = split.getStart() + split.getLength();
            fileIn.seek(split.getStart());
            dataIn = fileIn;
            this.fileIn = fileIn;
        } else if (codec instanceof SplittableCompressionCodec) {
            SplittableCompressionCodec splitCodec = (SplittableCompressionCodec) codec;
            SplitCompressionInputStream cIn = splitCodec.createInputStream(
                    fileIn, codec.createDecompressor(), split.getStart(), split
                            .getStart()
                            + split.getLength(),
                    SplittableCompressionCodec.READ_MODE.BYBLOCK);
            this.splitStart = cIn.getAdjustedStart();
            this.splitEnd = cIn.getAdjustedEnd();
            this.fileIn = cIn;
            this.dataIn = cIn;
        } else {
            // Classic compression codec, no splits....
            dataIn = fileIn;
            this.fileIn = fileIn;
        }
        lastStartPos = splitStart;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (closed) {
            return false;
        }

        currentValue.set("");

        // Step 1, seek to the next startSequence

        final byte buf[] = new byte[1];
        boolean start = false;
        int pos = 0;
        do {
            if (pos == 0 && fileIn.getPos() > splitEnd) {
                return false;
            }
            int read = dataIn.read(buf);
            if (read == -1) {
                close();
                return false;
            }
            if (buf[0] == startSequence[pos]) {
                pos++;
                if (pos == startSequence.length) {
                    start = true;
                }
            } else {
                pos = 0;
            }
        } while (!start);

        long startPos = fileIn.getPos();

        startRecord();
        out.write(startSequence);

        // Step 2, seek to the end sequence and write to the output buffer
        boolean end = false;
        pos = 0;
        do {
            int read = dataIn.read(buf);
            if (read == -1) {
                close();
                return false;
            }
            out.write(buf);
            if (valueBuffer.size() > maxRecordSize) {
                endRecord();
                return nextKeyValue();
            }
            if (buf[0] == endSequence[pos]) {
                pos++;
                if (pos == endSequence.length) {
                    end = true;
                }
            } else {
                pos = 0;
            }
        } while (!end);

        out.flush();
        byte[] bytes = valueBuffer.toByteArray();
        endRecord();

        // Step 3, compute a key / value pair
        currentValue.set(bytes);
        bytes = null;

        if (startPos != lastStartPos) {
            inputByteCounter.increment(startPos - lastStartPos);
            context.progress();
            lastStartPos = startPos;
            lastStartId = -1;
        }
        lastStartId++;
        currentKey.set(startPos * 60 + lastStartId);

        if (fileIn.getPos() >= splitEnd) {
            // We know that there are no new entries
            close();
        }

        return true;
    }

    public void startRecord() throws IOException {
        if (out != null && valueBuffer.size() == 0) {
            return;
        }
        valueBuffer = new ByteArrayOutputStream(1024*1024);
        out = valueBuffer;
    }

    public void endRecord() {
        if (valueBuffer != null) {
            if (valueBuffer.size() > 1024 * 1024 * 16) {
                valueBuffer = new ByteArrayOutputStream(1024*1024);
            } else {
                valueBuffer.reset();
            }
            out = valueBuffer;
            return;
        }
    }

}
