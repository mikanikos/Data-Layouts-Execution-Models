package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.Iterator;

public class Join implements ColumnarOperator {

	public ColumnarOperator leftChild;
	public ColumnarOperator rightChild;
	public int leftFieldNo;
	public int rightFieldNo;
	public Map<Object, List<DBTuple>> hashMap;

	public Join(ColumnarOperator leftChild, ColumnarOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
		this.hashMap = new HashMap<Object, List<DBTuple>>();
	}

	public DBColumn[] execute() {

		DBColumn[] leftOutput = leftChild.execute();
		DBColumn[] rightOutput = rightChild.execute();

		// if late materialization is enabled, I materialize both the left and right previous outputs

		// materialization of the left child
		if (leftOutput.length != 0) {
			if (leftOutput[0].useLateMaterialization) {
				List<DBColumn> listDBCol = new ArrayList<DBColumn>();
				for (int i = 0; i < leftOutput.length; i++) {
					DBColumn temp = leftOutput[i].materialize();
					if (!temp.eof)
						listDBCol.add(temp);
				}
				leftOutput = listDBCol.toArray(new DBColumn[0]);
			}
		}

		// materialization of the right child
		if (rightOutput.length != 0) {
			if (rightOutput[0].useLateMaterialization) {
				List<DBColumn> listDBCol = new ArrayList<DBColumn>();
				for (int i = 0; i < rightOutput.length; i++) {
					DBColumn temp = rightOutput[i].materialize();
					if (!temp.eof)
						listDBCol.add(temp);
				}
				rightOutput = listDBCol.toArray(new DBColumn[0]);
			}
		}

		// In order to apply a properly correct join, I convert DBColumns to DBTuples, perform hashjoin and then retrieve DBColumns again
		DBTuple[] leftTuples = convertColumnsInTuples(leftOutput);
		DBTuple[] rightTuples = convertColumnsInTuples(rightOutput);

		Object key;
		List<DBTuple> result = new ArrayList<DBTuple>();

		// processing left child tuples and building hash map
		for(DBTuple t : leftTuples) {
			key = ch.epfl.dias.ops.volcano.HashJoin.getKeyValue(t, t.types[leftFieldNo], leftFieldNo);
			List<DBTuple> listTuple = hashMap.getOrDefault(key, new ArrayList<>());
			listTuple.add(t);
			hashMap.put(key, listTuple);
		}

		// processing right child tuples and combining rows
		for(DBTuple t : rightTuples) {
			key = ch.epfl.dias.ops.volcano.HashJoin.getKeyValue(t, t.types[rightFieldNo], rightFieldNo);
			List<DBTuple> currentProcessingList = hashMap.get(key);

			if (currentProcessingList != null) {
				Iterator<DBTuple> iterator = currentProcessingList.iterator();
				while (iterator.hasNext())
					result.add(ch.epfl.dias.ops.volcano.HashJoin.combineTuples(iterator.next(), t));
			}
		}

		// converting back to DBColumns
		return convertTuplesinColumns(result.toArray(new DBTuple[0]));


	}


	// method to convert DbColumns in DBTuples
	public static DBTuple[] convertColumnsInTuples(DBColumn[] dbc) {

		List<List<Object>> listObjects = new ArrayList<List<Object>>();
		List<DataType> listTypes = new ArrayList<DataType>();


		for (int i = 0; i < dbc.length; i++) {
			for (int j = 0; j < dbc[i].values.length; j++) {
				if (j + 1 > listObjects.size()) {
					listObjects.add(j, new ArrayList<Object>());
				}
				listObjects.get(j).add(dbc[i].values[j]);
			}
		}

		for (int i = 0; i < dbc.length; i++) {
			listTypes.add(dbc[i].type);
		}

		DBTuple[] tuples = new DBTuple[listObjects.size()];
		for (int i = 0; i < listObjects.size(); i++) {
			tuples[i] = new DBTuple(listObjects.get(i).toArray(), listTypes.toArray(new DataType[0]));
		}

		return tuples;
	}


	// method to convert DBTuples in DBColumns
	public static DBColumn[] convertTuplesinColumns(DBTuple[] dbt) {

		List<DataType> listTypes = new ArrayList<DataType>();
		List<List<Object>> listObjects = new ArrayList<List<Object>>();

		for (int i = 0; i < dbt.length; i++) {
			for (int j = 0; j < dbt[i].fields.length; j++) {
				if (j + 1 > listObjects.size()) {
					listObjects.add(j, new ArrayList<Object>());
				}
				listObjects.get(j).add(dbt[i].fields[j]);
			}
		}

		for (int i = 0; i < dbt[0].fields.length; i++) {
			listTypes.add(dbt[0].types[i]);
		}


		DBColumn[] columns = new DBColumn[listObjects.size()];
		for (int i = 0; i < listObjects.size(); i++) {
			columns[i] = new DBColumn(listObjects.get(i).toArray(), listTypes.get(i), false);
		}

		return columns;
	}

}
