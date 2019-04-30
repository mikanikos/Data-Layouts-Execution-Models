package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;
import java.util.stream.Stream;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;


public class ProjectAggregate implements ColumnarOperator {

	public ColumnarOperator child;
	public Aggregate agg;
	public DataType dt;
	public int fieldNo;

	public ProjectAggregate(ColumnarOperator child, Aggregate agg, DataType dt, int fieldNo) {
		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;
	}

	@Override
	public DBColumn[] execute() {
		DBColumn[] previousOutput = child.execute();

		// if late materialization is enabled, I materialize columns before applying operation
		if (previousOutput.length != 0) {
			if (previousOutput[0].useLateMaterialization) {
				List<DBColumn> listDBCol = new ArrayList<DBColumn>();
				for (int i = 0; i < previousOutput.length; i++) {
					DBColumn temp = previousOutput[i].materialize();
					if (!temp.eof)
						listDBCol.add(temp);
				}

				previousOutput = listDBCol.toArray(new DBColumn[0]);
			}
		}

		DBColumn[] output = new DBColumn[1];
		output[0] = new DBColumn();

		DBColumn fieldSelected = previousOutput[fieldNo];

		List<Integer> listInteger;
		List<Double> listDouble;

		// Simply apply the right operation according to the parameters and return the result
		switch(this.agg) {

			case COUNT:
				List<Object> list = Arrays.asList(fieldSelected.values);
				output[0] = new DBColumn(new Object[]{new Integer(list.size())}, dt, false);
				break;

			case MAX:
				switch(this.dt) {
					case INT:
						listInteger = Arrays.asList(fieldSelected.getAsInteger());
						output[0] = new DBColumn(new Object[]{new Integer(listInteger.stream().mapToInt(v -> v).max().getAsInt())}, dt, false);
						break;

					case DOUBLE:
						listDouble = Arrays.asList(fieldSelected.getAsDouble());
						output[0] = new DBColumn(new Object[]{new Double(listDouble.stream().mapToDouble(v -> v).max().getAsDouble())}, dt, false);
						break;
				}
				break;

			case MIN:
				switch(this.dt) {
					case INT:
						listInteger = Arrays.asList(fieldSelected.getAsInteger());
						output[0] = new DBColumn(new Object[]{new Integer(listInteger.stream().mapToInt(v -> v).min().getAsInt())}, dt, false);
						break;

					case DOUBLE:
						listDouble = Arrays.asList(fieldSelected.getAsDouble());
						output[0] = new DBColumn(new Object[]{new Double(listDouble.stream().mapToDouble(v -> v).min().getAsDouble())}, dt, false);
						break;
				}
				break;

			case SUM:
				switch(this.dt) {
					case INT:
						listInteger = Arrays.asList(fieldSelected.getAsInteger());
						output[0] = new DBColumn(new Object[]{new Integer(listInteger.stream().mapToInt(v -> v).sum())}, dt, false);
						break;

					case DOUBLE:
						listDouble = Arrays.asList(fieldSelected.getAsDouble());
						output[0] = new DBColumn(new Object[]{new Double(listDouble.stream().mapToDouble(v -> v).sum())}, dt, false);
						break;
				}
				break;

			case AVG:
				switch(this.dt) {
					case INT:
						listInteger = Arrays.asList(fieldSelected.getAsInteger());
						output[0] = new DBColumn(new Object[]{new Double(listInteger.stream().mapToInt(v -> v).average().getAsDouble())}, dt, false);
						break;

					case DOUBLE:
						listDouble = Arrays.asList(fieldSelected.getAsDouble());
						output[0] = new DBColumn(new Object[]{new Double(listDouble.stream().mapToDouble(v -> v).average().getAsDouble())}, dt, false);
						break;
				}
				break;
		}

		return output;
	}
}
