package hadoopSecondSort;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.mortbay.log.Log;

public class HadoopSecondSortReducer extends Reducer<HadoopSecondSortKey, HadoopSecondSortKey, Text, Text> {
	
	@Override
	protected void reduce(HadoopSecondSortKey key,Iterable<HadoopSecondSortKey> values,
			Context context) throws IOException, InterruptedException{
		StringBuilder builder = new StringBuilder();
		for (HadoopSecondSortKey value:values){			
			builder.append("(");			
			builder.append(value.getYearMonth());
			builder.append(",");
			builder.append(value.getDay());        	 
			builder.append(")");			
		}
		
		context.write(new Text(key.getYearMonth()), new Text(builder.toString()));		
	}
}
