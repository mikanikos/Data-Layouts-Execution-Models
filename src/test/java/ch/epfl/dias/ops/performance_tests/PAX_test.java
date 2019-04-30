package ch.epfl.dias.ops.performance_tests;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.ops.volcano.HashJoin;
import ch.epfl.dias.ops.volcano.ProjectAggregate;
import ch.epfl.dias.ops.volcano.Select;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.PAX.PAXStore;
import ch.epfl.dias.store.row.DBTuple;
import ch.epfl.dias.store.row.RowStore;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class PAX_test {

    DataType[] orderSchema;
    DataType[] lineitemSchema;

    PAXStore rowstoreOrder;
    PAXStore rowstoreLineItem;

	// To enable testing on big dataset, set this to true and make sure your files are in the input folder
	public static final boolean USE_BIG_DATASETS = false;

	@Before
    public void init() throws IOException  {

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

        rowstoreOrder = new PAXStore(orderSchema, orders_data, "\\|", 10000);
        rowstoreOrder.load();

        rowstoreLineItem = new PAXStore(lineitemSchema, lineitem_data, "\\|", 10000);
        rowstoreLineItem.load();
    }

	@Test
	public void Sel_Proj_TestOrder(){

		ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
		Select sel = new Select(scan, BinaryOp.EQ, 0, 6);
		ch.epfl.dias.ops.volcano.Project pro = new ch.epfl.dias.ops.volcano.Project(sel, new int[] {0,1,2});

		pro.open();

		DBTuple result;
		while (!(result = pro.next()).eof);

	}

	@Test
	public void Join_Proj_Test(){

		ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(rowstoreOrder);
		ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
		HashJoin join = new HashJoin(scanOrder,scanLineitem,0,0);
		ch.epfl.dias.ops.volcano.Project pro = new ch.epfl.dias.ops.volcano.Project(join, new int[] {0,1,2});

		pro.open();

		DBTuple result;
		while (!(result = pro.next()).eof);

	}

	@Test
	public void Proj_Agg_TestLineItem(){

	    ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
		ProjectAggregate agg = new ProjectAggregate(scan, Aggregate.COUNT, DataType.INT, 2);
	
		agg.open();
		
		DBTuple result = agg.next();

	}

}
