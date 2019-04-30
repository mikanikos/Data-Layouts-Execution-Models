package ch.epfl.dias.store.column;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import ch.epfl.dias.store.DataType;

public class DBColumn {

	// values of the column
	public Object[] values;
	// bitmap vector in order to use late materialization
	public int[] bitmap;
	public DataType type;
	public boolean eof;
	public boolean useLateMaterialization;

	public DBColumn(Object[] values, DataType type, boolean useLateMaterialization) {
		this.values = values;
		this.bitmap = new int[values.length];
		Arrays.fill(bitmap, 1);
		this.type = type;
		this.eof = false;
		this.useLateMaterialization = useLateMaterialization;
	}

	public DBColumn() {
		this.eof = true;
	}
	

	public Integer[] getAsInteger() {

	    return Arrays.asList(values).toArray(new Integer[0]);
	}

	public Double[] getAsDouble() {

		return Arrays.asList(values).toArray(new Double[0]);
	}

	public Boolean[] getAsBoolean() {

		return Arrays.asList(values).toArray(new Boolean[0]);
	}

	public String[] getAsString() {

		return Arrays.asList(values).toArray(new String[0]);
	}


	// method to materialize columns when late materialization is used
	// scanning the values and retrieve a DBColumn that matches the 1-bit values of the bitmap
	public DBColumn materialize() {

		List<Object> objects = new ArrayList<Object>();
		for (int i = 0; i < values.length; i++) {
			if (this.bitmap[i] == 1)
				objects.add(values[i]);
		}

		if (objects.isEmpty())
		    return new DBColumn();
		else
		    return new DBColumn(objects.toArray(new Object[0]), this.type, false);

	}


}
