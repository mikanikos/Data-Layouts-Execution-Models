package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

public class Select implements VectorOperator {

	public VectorOperator child;
	public BinaryOp operation;
	public int fieldNo;
	public int value;
	public List<DBTuple> tuplesList;
	public int vectorSize;
	public int currentPos;
	public boolean checkFirstTime;

	public Select(VectorOperator child, BinaryOp op, int fieldNo, int value) {
		this.child = child;
		this.operation = op;
		this.fieldNo = fieldNo;
		this.value = value;
		this.currentPos = 0;
		this.checkFirstTime = true;
	}
	
	@Override
	public void open() {
		this.tuplesList = new ArrayList<DBTuple>();
		child.open();
	}

	@Override
	public DBColumn[] next() {
		DBColumn[] previousOut;

		// output vector
		DBColumn[] output = new DBColumn[] {new DBColumn()};

		// I keep retrieving vectors
		while (!(previousOut = child.next())[0].eof) {

			if (checkFirstTime) {
				this.vectorSize = previousOut[0].values.length;
				checkFirstTime = false;
			}

			// applying Select in the same way Column model does
			DBColumn[] outSelect = ch.epfl.dias.ops.columnar.Select.applySelect_early_materialization(previousOut, fieldNo, value, operation);

			// In order to return output in chunks, I store the result in a list and once I got n elements (vectorsize) I return, and so on until previous output ends
			// I convert to tuples because it's then easier to handle the buffer
			DBTuple[] tuplesSel = ch.epfl.dias.ops.columnar.Join.convertColumnsInTuples(outSelect);

			for (int i = 0; i < tuplesSel.length; i++) {
				tuplesList.add(tuplesSel[i]);

				if (tuplesList.size() == vectorSize) {
					output = ch.epfl.dias.ops.columnar.Join.convertTuplesinColumns(tuplesList.toArray(new DBTuple[0]));
					tuplesList = new ArrayList<DBTuple>();
				}
			}

			if (!output[0].eof)
				return output;
		}

		// if list is not empty, I still have items left and I retrieve them: now I finished processing
		if (!tuplesList.isEmpty()) {
            output = ch.epfl.dias.ops.columnar.Join.convertTuplesinColumns(tuplesList.toArray(new DBTuple[0]));
			tuplesList = new ArrayList<DBTuple>();
		}

		return output;

	}

	@Override
	public void close() {
		child.close();
	}
}
