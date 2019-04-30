package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;
import java.util.List;
import java.util.ArrayList;

public class Project implements VectorOperator {

	public VectorOperator child;
	public int[] columns;

	public Project(VectorOperator child, int[] fieldNo) {
		this.child = child;
		this.columns= fieldNo;
	}

	@Override
	public void open() {
		child.open();
	}

	@Override
	public DBColumn[] next() {

		// Project similar to Column model, vector doesn't change its size and there's no need of a buffer, so I return once I finished
		DBColumn[] previousOut = child.next();

		if (previousOut[0].eof)
			return new DBColumn[] {new DBColumn()};

		DBColumn[] output = new DBColumn[columns.length];

		for (int i = 0; i < columns.length; i++)
			output[i] = previousOut[columns[i]];

		return output;

	}

	@Override
	public void close() {
		child.close();
	}
}
