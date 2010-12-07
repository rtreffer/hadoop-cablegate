package com.github.cablegate.input;

import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class WikimediaSimplifyRecordReader extends MatchRecordReader {

	@Override
	public void endRecord() {
		if (out != null) {
			try {
				out.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		super.endRecord();
	}

	@Override
	public void startRecord() throws IOException {
		if (out != null) {
			try {
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			out = null;
		}
		super.startRecord();
		out = new TextMoveOutputStream(valueBuffer);
	}

	private final static class TextMoveOutputStream extends FilterOutputStream {

		private final static byte[] TEXT_START = "\n      <text xml:space=\"preserve\">"
		.getBytes();
		private final static byte[] PAGE_END = "\n  </page>".getBytes();
		private final static byte[] TEXT_END = "</text>".getBytes();

		private ByteArrayOutputStream textBuffer = null;
		private int text_end_pos = 0;
		private int text_start_pos = 0;
		private int page_end_pos = 0;

		private boolean intext = false;

		public TextMoveOutputStream(OutputStream out) {
			super(out);
		}

		@Override
		public void write(int b) throws IOException {
			if (intext) {
				if (b == TEXT_END[text_end_pos]) {
					text_end_pos++;
					if (text_end_pos == TEXT_END.length) {
						intext = false;
					}
				} else {
					if (text_end_pos == 0) {
						textBuffer.write(b);
					} else {
						textBuffer.write(TEXT_END, 0, text_end_pos);
						text_end_pos = 0;
					}
				}
				return;
			}
			if (b == TEXT_START[text_start_pos]) {
				text_start_pos++;
				if (text_start_pos == TEXT_START.length) {
					intext = true;
					textBuffer = new ByteArrayOutputStream();
					text_start_pos = 0;
					page_end_pos = 0;
					text_end_pos = 0;
					return;
				}
			} else if (text_start_pos != 0) {
				if (text_start_pos > page_end_pos) {
					for (int i = 0, l = text_start_pos - page_end_pos; i < l; i++) {
						super.write(TEXT_START[i]);
					}
				}
				text_start_pos = 0;
			}
			if (b == PAGE_END[page_end_pos]) {
				page_end_pos++;
				if (page_end_pos == PAGE_END.length) {
					if (textBuffer != null) {
						for (byte byt : TEXT_START) {
							super.write(byt);
						}
						for (byte byt : textBuffer.toByteArray()) {
							super.write(byt);
						}
						for (byte byt : TEXT_END) {
							super.write(byt);
						}
					}
					textBuffer = null;
					for (byte byt : PAGE_END) {
						super.write(byt);
					}
					page_end_pos = 0;
					text_start_pos = 0;
					text_end_pos = 0;
					return;
				}
			} else if (page_end_pos != 0) {
				if (text_start_pos < page_end_pos) {
					for (int i = 0, l = page_end_pos - text_start_pos; i < l; i++) {
						super.write(PAGE_END[i]);
					}
				}
				page_end_pos = 0;
			}
			if (text_start_pos == 0 && page_end_pos == 0) {
				super.write(b);
			}
		}

	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext ctx)
	throws IOException, InterruptedException {
		super.initialize(genericSplit, ctx);
		super.startSequence = "\n  <page>".getBytes();
		super.endSequence = "\n  </page>".getBytes();
	}
}
