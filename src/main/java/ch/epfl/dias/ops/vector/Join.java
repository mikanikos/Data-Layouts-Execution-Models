package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.Arrays;
import java.util.Iterator;

public class Join implements VectorOperator {

	public VectorOperator leftChild;
	public VectorOperator rightChild;
	public int leftFieldNo;
	public int rightFieldNo;
	public Map<Object, List<DBTuple>> hashMap;
	public List<DBTuple> tuplesList;
	public int vectorSize;
	public boolean checkFirstTime;
	public int currentPos;


	public Join(VectorOperator leftChild, VectorOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
		this.checkFirstTime = true;
		this.currentPos = 0;
	}

	@Override
	public void open() {
		this.hashMap = new HashMap<Object, List<DBTuple>>();
		this.tuplesList = new ArrayList<DBTuple>();
		leftChild.open();
		rightChild.open();
	}

	@Override
	public DBColumn[] next() {

		DBColumn[] leftOut;
		DBColumn[] rightOut;

		Object key;

		// In order to do properly a correct join, I convert DBColumns in DBTuples and I apply hashjoin as in the Volcano model

		// processing left child and building hash table
		while(!(leftOut = leftChild.next())[0].eof) {

			if (checkFirstTime) {
				this.vectorSize = leftOut[0].values.length;
				checkFirstTime = false;
			}

			DBTuple[] leftTuples = ch.epfl.dias.ops.columnar.Join.convertColumnsInTuples(leftOut);
			List<DBTuple> result = new ArrayList<DBTuple>();

			for(DBTuple t : leftTuples) {
				key = ch.epfl.dias.ops.volcano.HashJoin.getKeyValue(t, t.types[leftFieldNo], leftFieldNo);
				List<DBTuple> listTuple = hashMap.getOrDefault(key, new ArrayList<>());
				listTuple.add(t);
				hashMap.put(key, listTuple);
			}
		}

		// processing right child and combine tuples
		while(!(rightOut = rightChild.next())[0].eof) {

		    if (rightOut[0].values.length > vectorSize)
                this.vectorSize = rightOut[0].values.length;

			DBTuple[] rightTuples = ch.epfl.dias.ops.columnar.Join.convertColumnsInTuples(rightOut);
			for(DBTuple t : rightTuples) {
				key = ch.epfl.dias.ops.volcano.HashJoin.getKeyValue(t, t.types[rightFieldNo], rightFieldNo);
				List<DBTuple> currentProcessingList = hashMap.get(key);

				if (currentProcessingList != null) {
                    Iterator<DBTuple> iterator = currentProcessingList.iterator();
                    while (iterator.hasNext())
                        tuplesList.add(ch.epfl.dias.ops.volcano.HashJoin.combineTuples(iterator.next(), t));
                }
			}
		}


		// Once I completed the join, I return a vector by using a buffer always applied to tuples
		if (currentPos ==  tuplesList.size())
			return new DBColumn[] {new DBColumn()};

		int threshold = Math.min(tuplesList.size(), vectorSize + currentPos);
		List<DBTuple> temp = new ArrayList<DBTuple>();
		DBColumn[] output = new DBColumn[] {new DBColumn()};


		for (int i = currentPos; i < threshold; i++) {
			temp.add(tuplesList.get(i));
		}

		currentPos = threshold;

		// I finally convert the vector of tuples to columns
		return ch.epfl.dias.ops.columnar.Join.convertTuplesinColumns(temp.toArray(new DBTuple[0]));

	}

	@Override
	public void close() {
		leftChild.close();
		rightChild.close();
	}

}
