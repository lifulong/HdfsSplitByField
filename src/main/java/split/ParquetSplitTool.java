package split;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapred.AvroValue;

import java.io.IOException;
import java.util.List;

public class ParquetSplitTool {

	public static class ParquetMapper extends Mapper<Void, GenericRecord, Text, AvroValue<GenericRecord>> {
		Text keyEmit = new Text();
		private final AvroValue<GenericRecord> outputValue = new AvroValue<GenericRecord>();
		String splitField = null;

		@Override
		protected void setup(Context context) {
		        splitField = context.getConfiguration().get("splitField");
		}

		public void map(Void key, GenericRecord value, Context context) throws IOException, InterruptedException {
			// Split by split field
			Object keyNew = value.get(splitField);
			keyEmit.set(keyNew.toString());
			outputValue.datum(value);
			context.write(keyEmit, outputValue);
		}
	}

	public static class ParquetReducer extends Reducer<Text, AvroValue<GenericRecord>, Void, GenericRecord> {
		MultipleOutputs<Void, GenericRecord> mos;
		NullWritable out = NullWritable.get();

		@Override
		protected void setup(Context context) {
			mos = new MultipleOutputs(context);
		}

		@Override
		protected void cleanup(Context context) {
			try {
				mos.close();                                                                                                       
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	}

		public void reduce(Text key, Iterable<AvroValue<GenericRecord>> values, Context context) {
			for (AvroValue<GenericRecord> value : values) {
				try {
					mos.write((Void) null, value.datum(), key.toString());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("splitField", args[0]);
		Job job = Job.getInstance(conf, "ParquetSplitTool");
		job.setJarByClass(ParquetSplitTool.class);
		job.setMapperClass(ParquetMapper.class);
		job.setReducerClass(ParquetReducer.class);

		//get schema
		Path infile = new Path(args[1]);
		List<Footer> footers = ParquetFileReader.readFooters(conf, infile.getFileSystem(conf).getFileStatus(infile), true);
		MessageType schema = footers.get(0).getParquetMetadata().getFileMetaData().getSchema();
		System.out.println("Schema:" + schema);
		// Avro Schema
		// Convert the Parquet schema to an Avro schema
		AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter();
		Schema avroSchema = avroSchemaConverter.convert(schema);
		AvroParquetInputFormat.setAvroReadSchema(job, avroSchema);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(AvroValue.class);
		job.setOutputKeyClass(Void.class);
		job.setOutputValueClass(GenericRecord.class);
		AvroJob.setMapOutputValueSchema(job, avroSchema);

		job.setInputFormatClass(AvroParquetInputFormat.class);
		job.setOutputFormatClass(AvroParquetOutputFormat.class);
		AvroParquetOutputFormat.setOutputPath(job, new Path(args[1]));
		AvroParquetOutputFormat.setSchema(job, avroSchema);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

