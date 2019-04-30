package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import static ch.epfl.dias.ops.Aggregate.AVG;
import static ch.epfl.dias.ops.Aggregate.COUNT;

public class ProjectAggregate implements VectorOperator {

	public VectorOperator child;
	public Aggregate agg;
	public DataType dt;
	public int fieldNo;
	public Double result;
	public Integer avgCount;

	public ProjectAggregate(VectorOperator child, Aggregate agg, DataType dt, int fieldNo) {
		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		this.avgCount = new Integer(0);
		child.open();
	}

	@Override
	public DBColumn[] next() {
		DBColumn[] previousOutput;
		DBColumn result;

		List<Integer> listInteger;
		List<Double> listDouble;

		// I keep getting the output of the child and I process it along the way, the difference is that now I handle an array of columns
		while (!(previousOutput = child.next())[0].eof) {

			DBColumn fieldSelected = previousOutput[fieldNo];

			// Select correct operation and type
			switch(this.agg) {

				case COUNT:
					if (this.result == null) {
						this.result = new Double(0);
					}
					List<Object> list = Arrays.asList(fieldSelected.values);
					this.result += new Integer(list.size());
					break;

				case MAX:
					switch(this.dt) {
						case INT:
							listInteger = Arrays.asList(fieldSelected.getAsInteger());
							if (this.result == null) {
								this.result = new Double(listInteger.stream().mapToInt(v -> v).max().getAsInt());
							}
							else {
								this.result = new Double(Math.max(this.result, listInteger.stream().mapToInt(v -> v).max().getAsInt()));
							}
							break;

						case DOUBLE:
							listDouble = Arrays.asList(fieldSelected.getAsDouble());
							if (this.result == null) {
								this.result = new Double(listDouble.stream().mapToDouble(v -> v).max().getAsDouble());
							}
							else {
								this.result = new Double(Math.max(this.result, listDouble.stream().mapToDouble(v -> v).max().getAsDouble()));
							}
							break;
					}
					break;

				case MIN:
					switch(this.dt) {
						case INT:
							listInteger = Arrays.asList(fieldSelected.getAsInteger());
							if (this.result == null) {
								this.result = new Double(listInteger.stream().mapToInt(v -> v).min().getAsInt());
							}
							else {
								this.result = new Double(Math.min(this.result, listInteger.stream().mapToInt(v -> v).min().getAsInt()));
							}
							break;

						case DOUBLE:
							listDouble = Arrays.asList(fieldSelected.getAsDouble());
							if (this.result == null) {
								this.result = new Double(listDouble.stream().mapToDouble(v -> v).min().getAsDouble());
							}
							else {
								this.result = new Double(Math.min(this.result, listDouble.stream().mapToDouble(v -> v).min().getAsDouble()));
							}
							break;
					}
					break;

				case SUM:
					switch(this.dt) {
						case INT:
							listInteger = Arrays.asList(fieldSelected.getAsInteger());
							if (this.result == null) {
								this.result = new Double(listInteger.stream().mapToInt(v -> v).sum());
							}
							else {
								this.result = new Double(this.result + listInteger.stream().mapToInt(v -> v).sum());
							}
							break;

						case DOUBLE:
							listDouble = Arrays.asList(fieldSelected.getAsDouble());
							if (this.result == null) {
								this.result = new Double(listDouble.stream().mapToDouble(v -> v).sum());
							}
							else {
								this.result = new Double(this.result + listDouble.stream().mapToDouble(v -> v).sum());
							}
							break;
					}
					break;

				case AVG:
					if (this.avgCount == null) {
						this.avgCount = new Integer(0);
					}
					List<Object> l = Arrays.asList(fieldSelected.values);

					this.avgCount += new Integer(l.size());

					switch(this.dt) {
						case INT:
							listInteger = Arrays.asList(fieldSelected.getAsInteger());
							if (this.result == null) {
								this.result = new Double(listInteger.stream().mapToInt(v -> v).sum());
							}
							else {
								this.result = new Double(this.result + listInteger.stream().mapToInt(v -> v).sum());
							}
							break;

						case DOUBLE:
							listDouble = Arrays.asList(fieldSelected.getAsDouble());
							if (this.result == null) {
								this.result = new Double(listDouble.stream().mapToDouble(v -> v).sum());
							}
							else {
								this.result = new Double(this.result + listDouble.stream().mapToDouble(v -> v).sum());
							}
							break;
					}
					break;
			}
		}


		// If average, I return a double
		if (this.agg == AVG) {
			if (this.avgCount.intValue() == 0)
				return new DBColumn[] {new DBColumn()};
			else
				this.result = new Double(this.result.doubleValue() / this.avgCount.intValue());

			return new DBColumn[] {new DBColumn(new Object[]{this.result.doubleValue()}, DataType.DOUBLE, false)};
		}

		// if count, I return an int
		if (this.agg == COUNT) {

			return new DBColumn[] {new DBColumn(new Object[]{this.result.intValue()}, DataType.INT, false)};
		}

		// Choosing the right Datatype
		switch(dt) {
			case DOUBLE:
				result = new DBColumn(new Object[]{this.result.doubleValue()}, DataType.DOUBLE, false);
				break;

			default:
				result = new DBColumn(new Object[]{this.result.intValue()}, DataType.INT, false);
		}

		return new DBColumn[] {result};
	}

	@Override
	public void close() {
		child.close();
	}

}
