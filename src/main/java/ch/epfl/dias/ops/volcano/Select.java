package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.row.DBTuple;

public class Select implements VolcanoOperator {

	public VolcanoOperator child;
	public BinaryOp operation;
	public int fieldNo;
	public int value;


	public Select(VolcanoOperator child, BinaryOp op, int fieldNo, int value) {
		this.child = child;
		this.operation = op;
		this.fieldNo = fieldNo;
		this.value = value;
	}

	@Override
	public void open() {
		child.open();
	}

	@Override
	public DBTuple next() {
		DBTuple t;
        int fieldValue;

        // Keep getting child output and simply process the select
        while (!(t = child.next()).eof) {
		    fieldValue = t.getFieldAsInt(fieldNo).intValue();

			switch(this.operation) {
				case LT:
					if (fieldValue < this.value)
						return t;
					break;
				case LE:
					if (fieldValue <= this.value)
						return t;
					break;
				case EQ:
					if (fieldValue == this.value)
						return t;
					break;
				case NE:
					if (fieldValue != this.value)
						return t;
					break;
				case GT:
					if (fieldValue > this.value)
						return t;
					break;
				case GE:
					if (fieldValue >= this.value)
						return t;
					break;
			}
		}

		return t;
	}

	@Override
	public void close() {
		child.close();
	}
}
