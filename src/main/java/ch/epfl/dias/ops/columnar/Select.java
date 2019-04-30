package ch.epfl.dias.ops.columnar;

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.column.DBColumn;

public class Select implements ColumnarOperator {

	public ColumnarOperator child;
	public BinaryOp operation;
	public int fieldNo;
	public int value;

	public Select(ColumnarOperator child, BinaryOp op, int fieldNo, int value) {
		this.child = child;
		this.operation = op;
		this.fieldNo = fieldNo;
		this.value = value;
	}

	@Override
	public DBColumn[] execute() {
		DBColumn[] previousOut = child.execute();

		// if late materialization is enabled, I apply that
		if (previousOut.length != 0) {
			if (previousOut[0].useLateMaterialization)
				return applySelect_late_materialization(previousOut, fieldNo, value, operation);

		}

		// otherwise, apply standard select
		return applySelect_early_materialization(previousOut, fieldNo, value, operation);

	}

	// method to apply select operation, simply iterating over all columns and values
	public static DBColumn[] applySelect_early_materialization(DBColumn[] input, int fieldNo, int value, BinaryOp operation) {
		DBColumn[] output = new DBColumn[input.length];

		// select indixes of the values to select
		List<Integer> selectedIndexes = getSelectedIndexes(input, fieldNo, value, operation);

		// once I have the desired indexes, I iterate over the columns and values to get what I want
		List<Object> objects;
		for (int i = 0; i < input.length; i++) {
			objects = new ArrayList<Object>();
			for (int j = 0; j < input[i].values.length; j++) {
				if (selectedIndexes.contains(j)) {
					objects.add(input[i].values[j]);
				}
			}

			output[i] = new DBColumn(objects.toArray(), input[i].type, false);
		}

		return output;
	}


	// applying select with late materialization, same approach of the previous method but operating on the bitmaps
	public static DBColumn[] applySelect_late_materialization(DBColumn[] input, int fieldNo, int value, BinaryOp operation) {

		List<Integer> selectedIndexes = getSelectedIndexes(input, fieldNo, value, operation);

		for (int i = 0; i < input.length; i++) {
			for (int j = 0; j < input[i].values.length; j++) {
				if (!selectedIndexes.contains(j)) {
					input[i].bitmap[j] = 0;
				}
			}
		}

		return input;
	}


	// get selected indexes according to operation and value
	public static List<Integer> getSelectedIndexes(DBColumn[] input, int fieldNo, int value, BinaryOp operation) {

		Integer[] fieldSelectInt = input[fieldNo].getAsInteger();
		List<Integer> selectedIndexes = new ArrayList<Integer>();

		int currentValue;

		for (int i = 0; i < fieldSelectInt.length; i++) {
			currentValue = fieldSelectInt[i].intValue();

			switch(operation) {
				case LT:
					if (currentValue < value)
						selectedIndexes.add(i);
					break;
				case LE:
					if (currentValue <= value)
						selectedIndexes.add(i);
					break;
				case EQ:
					if (currentValue == value)
						selectedIndexes.add(i);
					break;
				case NE:
					if (currentValue != value)
						selectedIndexes.add(i);
					break;
				case GT:
					if (currentValue > value)
						selectedIndexes.add(i);
					break;
				case GE:
					if (currentValue >= value)
						selectedIndexes.add(i);
					break;
			}
		}

		return selectedIndexes;
	}



}
