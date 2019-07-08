package hadoopSecondSort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class HadoopSecondSortValue implements Writable, Comparable<HadoopSecondSortValue> {

	private int day;
	private Long temperature;
	
	
	public static HadoopSecondSortValue  copy(HadoopSecondSortValue  value) {
		return new HadoopSecondSortValue(value.day, value.temperature);
	}
	
	public HadoopSecondSortValue(int day,Long temperature){
		set(day,temperature);
	}
	
	public HadoopSecondSortValue(){}
	
	public void set(int day,Long temperature){
		this.day = day;
		this.temperature = temperature;
	}
	
	public int getDay() {
		return this.day;
	}

	public Long getTemperature() {
		return this.temperature;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.day = in.readInt();
		this.temperature = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.day);
		out.writeDouble(this.temperature);
	}


	
	public static HadoopSecondSortValue read(DataInput in) throws IOException {
		HadoopSecondSortValue value = new HadoopSecondSortValue();
		value.readFields(in);
		return value;
	}
	
	public HadoopSecondSortValue clone() {
	       return new HadoopSecondSortValue(day, temperature);
	    }

	
	
	@Override
	public int compareTo(HadoopSecondSortValue data) {
		if(this.day>data.day){
			return -1;
		}
		else if (this.day<data.day){
			return 1;
		}else{
			return 0;
		}
	}


}
