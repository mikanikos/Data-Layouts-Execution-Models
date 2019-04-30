package ch.epfl.dias.ops.vector;

import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import java.util.List;
import java.util.ArrayList;

import java.lang.Math;

public class Scan implements VectorOperator {

	public ColumnStore store;
	public int vectorsize;
	public int currentPos;
	public List<DBColumn> colsList;

	public Scan(Store store, int vectorsize) {
		this.store = (ColumnStore) store;
		this.vectorsize = vectorsize;
		this.currentPos = 0;
	}
	
	@Override
	public void open() {
		colsList = new ArrayList<DBColumn>();
	}

	@Override
	public DBColumn[] next() {

		// if data structure is empty, I retrieve the columns I have in the storage
	    if (colsList.isEmpty()) {
            colsList = store.columns;
        }

        DBColumn[] output = new DBColumn[colsList.size()];

	    // I return columns in chunks according to the vector size, last exceeding columns are retrieved in a smaller vector
        if (currentPos ==  colsList.get(0).values.length)
            return new DBColumn[] {new DBColumn()};

        int threshold = Math.min(currentPos + vectorsize, colsList.get(0).values.length);

        for (int i = 0; i < colsList.size(); i++) {
            List<Object> objects = new ArrayList<Object>();
            for (int j = currentPos; j < threshold; j++)
                objects.add(colsList.get(i).values[j]);
            output[i] = new DBColumn(objects.toArray(), colsList.get(i).type, false);
        }

        currentPos = threshold;

        return output;

	}

	@Override
	public void close() {
	}
}
