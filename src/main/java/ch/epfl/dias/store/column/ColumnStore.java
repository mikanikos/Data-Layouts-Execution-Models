package ch.epfl.dias.store.column;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

public class ColumnStore extends Store {

	public List<DBColumn> columns;
	public DataType[] schema;
	public String filename;
	public String delimiter;
	public boolean lateMaterialization;

	public ColumnStore(DataType[] schema, String filename, String delimiter) {
		this(schema, filename, delimiter, false);
	}

	public ColumnStore(DataType[] schema, String filename, String delimiter, boolean lateMaterialization) {
		this.columns = new ArrayList<DBColumn>();
		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
		this.lateMaterialization = lateMaterialization;
	}

	@Override
	public void load() throws IOException {

		List<List<Object>> colObject = new ArrayList<List<Object>>();

		String line = "";

		BufferedReader b = null;

		// scanning and parsing the input, storing values in a list of lists and then creating DBColumns and saving them
		try {

			b = new BufferedReader(new FileReader(filename));

			while ((line = b.readLine()) != null) {

				String[] fields = line.split(delimiter);

                if (fields.length != 0) {
                    Object object;

                    for (int i = 0; i < fields.length; i++) {

                        switch(schema[i]) {
                            case INT:
                                object = new Integer(Integer.parseInt(fields[i]));
                                break;
                            case DOUBLE:
                                object = new Double(Double.parseDouble(fields[i]));
                                break;
                            case BOOLEAN:
                                object = new Boolean(Boolean.valueOf(fields[i]));
                                break;
                            default:
                                object = fields[i];
                        }

                        if (i+1 > colObject.size())
                            colObject.add(i, new ArrayList<Object>());
                        colObject.get(i).add(object);

                    }
                }

			}
		} catch (IOException e) {
			// If the file doesn't exist, I throw an exception as recommended on Moodle
            e.printStackTrace();
		}
		finally
		{
			try
			{
				b.close();
			}
			catch(IOException e)
			{
				System.out.println("Error in closing the buffer\n");
			}
		}

		// Creating DBColumns
		for (int i= 0; i < colObject.size(); i++) {
			if (lateMaterialization)
				columns.add(new DBColumn(colObject.get(i).toArray(), schema[i], true));
			else
				columns.add(new DBColumn(colObject.get(i).toArray(), schema[i], false));
		}

	}

	@Override
	public DBColumn[] getColumns(int[] columnsToGet) {
		DBColumn[] requested = new DBColumn[columnsToGet.length];
		for (int i = 0; i < columnsToGet.length; i++)
			requested[i] = columns.get(columnsToGet[i]);
		if (requested.length == 0)
			return new DBColumn[] {new DBColumn()};
		else
			return requested;
	}

}
