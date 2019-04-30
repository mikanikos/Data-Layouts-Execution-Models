package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements VolcanoOperator {

	public VolcanoOperator child;
	public int[] fieldNo;

	public Project(VolcanoOperator child, int[] fieldNo) {
		this.child = child;
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		child.open();
	}

	@Override
	public DBTuple next() {
		DBTuple t;
		int i;
		Object[] values = new Object[fieldNo.length];
		DataType[] types = new DataType[fieldNo.length];

		// Keep getting child output and process project operation on the indicated field
		while (!(t = child.next()).eof) {
			i = 0;
			for (int field : fieldNo) {
				DataType dt = t.types[field];
				switch(dt) {
					case INT:
						values[i] = t.getFieldAsInt(field);
						break;
					case DOUBLE:
						values[i] = t.getFieldAsDouble(field);
						break;
					case STRING:
						values[i] = t.getFieldAsString(field);
						break;
					case BOOLEAN:
						values[i] = t.getFieldAsBoolean(field);
						break;
				}
				types[i] = dt;
				i++;
			}
			return new DBTuple(values, types);
		}

		return t;
	}

	@Override
	public void close() {
		child.close();
	}
}
