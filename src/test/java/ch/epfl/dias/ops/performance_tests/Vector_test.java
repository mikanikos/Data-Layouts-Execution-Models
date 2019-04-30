package ch.epfl.dias.ops.performance_tests;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class Vector_test {

	DataType[] orderSchema;
	DataType[] lineitemSchema;

	ColumnStore columnstoreOrder;
	ColumnStore columnstoreLineItem;

	// To enable testing on big dataset, set this to true and make sure your files are in the input folder
	public static final boolean USE_BIG_DATASETS = false;

	@Before
	public void init() throws IOException {

		orderSchema = new DataType[]{
				DataType.INT,
				DataType.INT,
				DataType.STRING,
				DataType.DOUBLE,
				DataType.STRING,
				DataType.STRING,
				DataType.STRING,
				DataType.INT,
				DataType.STRING};

		lineitemSchema = new DataType[]{
				DataType.INT,
				DataType.INT,
				DataType.INT,
				DataType.INT,
				DataType.INT,
				DataType.DOUBLE,
				DataType.DOUBLE,
				DataType.DOUBLE,
				DataType.STRING,
				DataType.STRING,
				DataType.STRING,
				DataType.STRING,
				DataType.STRING,
				DataType.STRING,
				DataType.STRING,
				DataType.STRING};


		String orders_data;
		String lineitem_data;

		if (USE_BIG_DATASETS) {
			orders_data = "input/orders_big.csv";
			lineitem_data = "input/lineitem_big.csv";
		}
		else {
			orders_data = "input/orders_small.csv";
			lineitem_data = "input/lineitem_small.csv";
		}

		columnstoreOrder = new ColumnStore(orderSchema, orders_data, "\\|");
		columnstoreOrder.load();

		columnstoreLineItem = new ColumnStore(lineitemSchema, lineitem_data, "\\|");
		columnstoreLineItem.load();
	}

	@Test
	public void Sel_Proj_TestOrder(){

		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, 10000);
		ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 0, 6);
		ch.epfl.dias.ops.vector.Project pro = new ch.epfl.dias.ops.vector.Project(sel, new int[] {0,1,2});

		pro.open();

		DBColumn[] result;
		while (!(result = pro.next())[0].eof);

	}

	@Test
	public void Join_Proj_Test(){

		ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, 10000);
		ch.epfl.dias.ops.vector.Scan scanLineitem = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, 10000);
		ch.epfl.dias.ops.vector.Join join = new ch.epfl.dias.ops.vector.Join(scanOrder,scanLineitem,0,0);
		ch.epfl.dias.ops.vector.Project pro = new ch.epfl.dias.ops.vector.Project(join, new int[] {0,1,2});

		pro.open();

		DBColumn[] result;
		while (!(result = pro.next())[0].eof);

	}

	@Test
	public void Proj_Agg_TestLineItem(){

		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, 10000);
		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.COUNT, DataType.INT, 2);

		agg.open();

		DBColumn[] result = agg.next();

	}
}