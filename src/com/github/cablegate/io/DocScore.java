package com.github.cablegate.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DocScore implements Writable {

    private String document;
    private double score;

    public DocScore() {
    }

    public DocScore(String document, double score) {
        super();
        this.document = document;
        this.score = score;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        score = in.readDouble();
        document = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(score);
        out.writeUTF(document);
    }

    public String getDocument() {
        return document;
    }

    public void setDocument(String document) {
        this.document = document;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

}
