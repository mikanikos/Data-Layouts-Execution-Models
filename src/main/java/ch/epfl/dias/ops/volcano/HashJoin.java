package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.Iterator;
import java.util.Arrays;

public class HashJoin implements VolcanoOperator {

	public VolcanoOperator leftChild;
	public VolcanoOperator rightChild;
	public int leftFieldNo;
	public int rightFieldNo;
	public Map<Object, List<DBTuple>> hashMap;
	public Iterator<DBTuple> iterator;
	public DBTuple currentRightTuple;

	public HashJoin(VolcanoOperator leftChild, VolcanoOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
	}

	@Override
	public void open() {
		this.hashMap = new HashMap<Object, List<DBTuple>>();
		this.leftChild.open();
		this.rightChild.open();
	}

	@Override
	public DBTuple next() {
		DBTuple t;
		Object key;

		// Comuting HashJoin: first process the left child and store values in a hash map using the join column as key
		while(iterator == null && !(t = leftChild.next()).eof) {
			key = getKeyValue(t, t.types[leftFieldNo], leftFieldNo);
			List<DBTuple> listTuple = hashMap.getOrDefault(key, new ArrayList<>());
			listTuple.add(t);
			hashMap.put(key, listTuple);
		}

		// If I have already started the join on a column, I continue from here
		if (iterator != null && iterator.hasNext())
			return combineTuples(iterator.next(), currentRightTuple);

		// Processing the right child: retrieve the list corresponding to the key and then merge rows
		while(!(t = rightChild.next()).eof) {
			key = getKeyValue(t, t.types[rightFieldNo], rightFieldNo);
			List<DBTuple> currentProcessingList = hashMap.get(key);

			if (currentProcessingList != null) {
                iterator = currentProcessingList.iterator();
                currentRightTuple = t;
                if (iterator.hasNext())
                    return combineTuples(iterator.next(), currentRightTuple);
            }
		}

		return t;
	}

	@Override
	public void close() {
		this.leftChild.close();
		this.rightChild.close();
	}


	// Method to merge two tuples
	public static DBTuple combineTuples(DBTuple left, DBTuple right) {
		List<Object> fieldsTotal = new ArrayList<Object>();
		fieldsTotal.addAll(Arrays.asList(left.fields));
		fieldsTotal.addAll(Arrays.asList(right.fields));
		List<DataType> typesTotal = new ArrayList<DataType>();
		typesTotal.addAll(Arrays.asList(left.types));
		typesTotal.addAll(Arrays.asList(right.types));
		return new DBTuple(fieldsTotal.toArray(),  typesTotal.stream().toArray(DataType[]::new));
	}


	// Getting key value according to the DataType
	public static Object getKeyValue(DBTuple t, DataType dt, int field) {
		Object key = new Object();

		switch(dt) {
			case INT:
				key = (Object) t.getFieldAsInt(field);
				break;
			case DOUBLE:
				key = (Object) t.getFieldAsDouble(field);
				break;
			case STRING:
				key = (Object) t.getFieldAsString(field);
				break;
			case BOOLEAN:
				key = (Object) t.getFieldAsBoolean(field);
				break;
		}

		return key;
	}

}
