package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class Scan implements VolcanoOperator {

	public Store store;
	// index of the next row to retrieve, incremented on each scan
	public int current;

	public Scan(Store store) {
		this.store = store;
		this.current = -1;
	}

	@Override
	public void open() {
	}

	@Override
	public DBTuple next() {
		this.current++;
		return store.getRow(this.current);
	}

	@Override
	public void close() {
	}
}