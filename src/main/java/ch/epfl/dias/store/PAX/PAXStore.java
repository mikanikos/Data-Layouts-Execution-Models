package ch.epfl.dias.store.PAX;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class PAXStore extends Store {

	public List<DBPAXpage> pages;
	public DataType[] schema;
	public String filename;
	public String delimiter;
	public int tuplesPerPage;


	public PAXStore(DataType[] schema, String filename, String delimiter, int tuplesPerPage) {
		this.pages = new ArrayList<DBPAXpage>();
		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
		this.tuplesPerPage = tuplesPerPage;
	}

	@Override
	public void load() throws IOException {

		List<DBTuple> tuples = new ArrayList<DBTuple>();

		String line = "";

		BufferedReader b = null;

		// similar approach to the RowStore, in the end creating pages
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
				//e.printStackTrace();
			}
		}


		// After getting all the tuple, create the pages and store in them as many tuples as the passed parameter
		List<DBTuple> temp = new ArrayList<DBTuple>();

		for (int j = 0; j < tuples.size(); j++) {
			temp.add(tuples.get(j));
			if (temp.size() == tuplesPerPage) {
				pages.add(new DBPAXpage(temp.toArray(new DBTuple[0])));
				temp = new ArrayList<DBTuple>();
			}
		}

		if (!temp.isEmpty()) {
			pages.add(new DBPAXpage(temp.toArray(new DBTuple[0])));
		}


	}

	@Override
	public DBTuple getRow(int rownumber) {

		// Fast method to retrieve a tuple by computing the exact page and offset from the contants
		int page = 0;
		int offset = 0;

		boolean found = false;
		for (int i = 1; i <= pages.size(); i++) {
			if (i * tuplesPerPage > rownumber && pages.get(i-1).fields.length + ((i-1) * tuplesPerPage) > rownumber) {
				page = i - 1;
				offset = rownumber - ((i-1) * tuplesPerPage);
				found = true;
				break;
			}
		}

		if (!found)
			return new DBTuple();


		return pages.get(page).getDBTuple(offset);

	}
}
