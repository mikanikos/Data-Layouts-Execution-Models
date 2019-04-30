package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import java.util.List;

public class Scan implements ColumnarOperator {

	public ColumnStore store;

	public Scan(ColumnStore store) {
		this.store = store;
	}

	@Override
	public DBColumn[] execute() {
		// Simply retrieve all the columns in the storage
		List<DBColumn> columns = store.columns;
		return columns.toArray(new DBColumn[0]);
	}
}
