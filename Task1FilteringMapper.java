package mapreduce.task1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Task1FilteringMapper extends Mapper<LongWritable, Text, Text, Text>{
	public static final String NA_STRING = "NA";
	
	
	
	public void map(LongWritable lineOffsetInFile, Text recordInput, Context context) 
			throws IOException, InterruptedException {
		String[] recordFields = recordInput.toString().split("\\|");
		String companyName = recordFields[0].trim();
		String productName = recordFields[1].trim();
		if(!NA_STRING.equalsIgnoreCase(companyName) && 
				!NA_STRING.equalsIgnoreCase(productName) ) {
			context.write(recordInput, new Text());
		}
	} 
}
