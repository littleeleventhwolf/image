package com.image.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

public class ImageFileOutputFormat extends FileOutputFormat<Text,BytesWritable> {
 	@Override
 	public RecordWriter<Text, BytesWritable> getRecordWriter(TaskAttemptContext taskAttemptContext)
                                                 throws IOException, InterruptedException {
 		Configuration configuration = taskAttemptContext.getConfiguration();
 		Path path = getDefaultWorkFile(taskAttemptContext, "");
 		FileSystem fileSystem = path.getFileSystem(configuration);
 		FSDataOutputStream out = fileSystem.create(path,false);
 		return new ImageFileRecordWriter(out);
 	}

 	protected class ImageFileRecordWriter extends RecordWriter<Text, BytesWritable>{
 
 		protected DataOutputStream out;
 		private final byte[] keyValueSeparator;
 		private static final String colon=",";
 
 		public ImageFileRecordWriter(DataOutputStream out){
 			this(colon,out);
 		}
 
 		public ImageFileRecordWriter(String keyValueSeparator,DataOutputStream out) {
 			this.out=out;
 			this.keyValueSeparator = keyValueSeparator.getBytes();
 		}
 
 		@Override
 		public void write(Text text, BytesWritable bytesWritable) throws IOException, InterruptedException {
 			if(bytesWritable!=null){
 				out.write(bytesWritable.getBytes());
 			}
 		}
 
 		@Override
 		public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
 			out.close();
 		}
 	}
}