package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.store.column.DBColumn;

public class Project implements ColumnarOperator {

	public ColumnarOperator child;
	public int[] columns;

	public Project(ColumnarOperator child, int[] columns) {
		this.child = child;
		this.columns= columns;
	}

	public DBColumn[] execute() {
		DBColumn[] previousOut = child.execute();

		// if late materialization is enable, I perform the project operation on the bitmaps by putting to zero the non-selected columns
		if (previousOut.length != 0) {
			if (previousOut[0].useLateMaterialization) {
				int[] tempBitmap = new int[previousOut.length];
				for (int i = 0; i < columns.length; i++)
					tempBitmap[columns[i]] = 1;

				for (int i = 0; i < tempBitmap.length; i++)
					if (tempBitmap[i] == 0)
						previousOut[i].bitmap = new int[previousOut[i].values.length];

				return previousOut;
			}

		}

		// apply simple project to columns
		DBColumn[] output = new DBColumn[columns.length];

		for (int i = 0; i < columns.length; i++)
			output[i] = previousOut[columns[i]];

		return output;
	}
}
