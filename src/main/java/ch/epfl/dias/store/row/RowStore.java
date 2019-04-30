package ch.epfl.dias.store.row;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

public class RowStore extends Store {

	private List<DBTuple> tuples;
	private DataType[] schema;
	private String filename;
	private String delimiter;


	public RowStore(DataType[] schema, String filename, String delimiter) {
		this.tuples = new ArrayList<DBTuple>();
		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
	}

	@Override
	public void load() throws IOException {

		String line = "";

		BufferedReader b = null;

		// Using Buffer Reader to parse the input, create a DBTuple for each row and save tuples
		try {
			b = new BufferedReader(new FileReader(filename));

			while ((line = b.readLine()) != null) {

				String[] fields = line.split(delimiter);

				if (fields.length != 0) {

					Object[] objects = new Object[fields.length];

					for(int i = 0; i < fields.length; i++) {
						switch(schema[i]) {
							case INT:
								objects[i] = new Integer(Integer.parseInt(fields[i]));
								break;
							case DOUBLE:
								objects[i] = new Double(Double.parseDouble(fields[i]));
								break;
							case BOOLEAN:
								objects[i] = new Boolean(Boolean.valueOf(fields[i]));
								break;
							case STRING:
								objects[i] = fields[i];
								break;
						}
					}

					tuples.add(new DBTuple(objects, schema));
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

	}

	@Override
	public DBTuple getRow(int rownumber) {
		// End of tuples, I return EOF
		if (rownumber == tuples.size())
			return new DBTuple();
		else
			return tuples.get(rownumber);
	}
}
