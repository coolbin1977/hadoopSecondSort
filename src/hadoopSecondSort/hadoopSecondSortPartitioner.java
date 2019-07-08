package hadoopSecondSort;

import org.apache.hadoop.mapreduce.Partitioner;

public class hadoopSecondSortPartitioner extends Partitioner<HadoopSecondSortKey,HadoopSecondSortKey>{

	@Override
	public int getPartition(HadoopSecondSortKey key, HadoopSecondSortKey value, int numberOfPartitions) {
		
		return Math.abs((key.getYearMonth().hashCode())%numberOfPartitions);
	}

}
