package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

import static ch.epfl.dias.ops.Aggregate.AVG;
import static ch.epfl.dias.ops.Aggregate.COUNT;

public class ProjectAggregate implements VolcanoOperator {

	public VolcanoOperator child;
	public Aggregate agg;
	public DataType dt;
	public int fieldNo;
	public Double result;
	public Integer avgCount;


	public ProjectAggregate(VolcanoOperator child, Aggregate agg, DataType dt, int fieldNo) {
		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		this.avgCount = new Integer(0);
		child.open();
	}

	@Override
	public DBTuple next() {
		DBTuple t;
		Integer tempInt = new Integer(0);
		Double tempDouble = new Double(0);

		// From child output, check the Operation and the Datatype and updating result variable
		while (!(t = child.next()).eof) {
			switch(this.agg) {
				case COUNT:
					if (this.result == null) {
						this.result = new Double(0);
					}
					this.result = new Double(result.intValue() + 1);
					break;

				case MAX:
					switch(dt) {
						case INT:
                            tempInt = t.getFieldAsInt(fieldNo);
							if (this.result == null) {
								this.result = new Double(tempInt.intValue());
							}
							else {
                                if (this.result.intValue() < tempInt.intValue())
                                    this.result = new Double(tempInt.intValue());
                            }
							break;

						case DOUBLE:
                            tempDouble = t.getFieldAsDouble(fieldNo);
                            if (this.result == null) {
                                this.result = new Double(tempDouble.doubleValue());
                            }
                            else {
                                if (this.result.doubleValue() < tempDouble.doubleValue())
                                    this.result = tempDouble;
                            }
							break;
					}
					break;

				case MIN:
					switch(dt) {
						case INT:
                            tempInt = t.getFieldAsInt(fieldNo);
                            if (this.result == null) {
                                this.result = new Double(tempInt.intValue());
                            }
                            else {
                                if (this.result.intValue() > tempInt.intValue())
                                    this.result = new Double(tempInt.intValue());
                            }
							break;

						case DOUBLE:
                            tempDouble = t.getFieldAsDouble(fieldNo);
						    if (this.result == null) {
								this.result = new Double(tempDouble.doubleValue());
							}
						    else {
                                if (this.result.doubleValue() > tempDouble.doubleValue())
                                    this.result = tempDouble;
                            }
							break;
					}
					break;

				case SUM:
					switch(dt) {
						case INT:
                            tempInt = t.getFieldAsInt(fieldNo);
						    if (this.result == null) {
								this.result = new Double(tempInt.intValue());
							}
						    else {
                                this.result = new Double(tempInt.intValue() + result.intValue());
                            }
							break;

						case DOUBLE:
                            tempDouble = t.getFieldAsDouble(fieldNo);
						    if (this.result == null) {
								this.result = new Double(tempDouble.doubleValue());
							}
							else {
                                this.result = new Double(tempDouble.doubleValue() + result.doubleValue());
                            }
							break;
					}
					break;

				case AVG:
                    if (this.avgCount == null) {
                        this.avgCount = new Integer(0);
                    }
                    this.avgCount= new Integer(avgCount.intValue() + 1);
					switch(dt) {
                        case INT:
                            tempInt = t.getFieldAsInt(fieldNo);
                            if (this.result == null) {
                                this.result = new Double(tempInt.intValue());
                            }
                            else {
                                this.result = new Double(tempInt.intValue() + result.intValue());
                            }
                            break;

                        case DOUBLE:
                            tempDouble = t.getFieldAsDouble(fieldNo);
                            if (this.result == null) {
                                this.result = new Double(tempDouble.doubleValue());
                            }
                            else {
                                this.result = new Double(tempDouble.doubleValue() + result.doubleValue());
                            }
                            break;
                    }
                    break;
			}
		}

		// if Average, I return a double
		if (this.agg == AVG) {
			if (this.avgCount.intValue() == 0)
				return new DBTuple();
			else
				this.result = new Double(this.result.doubleValue() / this.avgCount.intValue());

			return new DBTuple(new Object[]{this.result.doubleValue()}, new DataType[]{DataType.DOUBLE});
		}

		// If Count, I return an INT
        if (this.agg == COUNT) {

            return new DBTuple(new Object[]{this.result.intValue()}, new DataType[]{DataType.INT});
        }


        // final result tuple
		DBTuple finalTuple = new DBTuple();

        // Converting result variable to the appropriate type
		switch(dt) {
			case DOUBLE:
				finalTuple = new DBTuple(new Object[]{this.result.doubleValue()}, new DataType[]{DataType.DOUBLE});
				break;

			default:
				finalTuple = new DBTuple(new Object[]{this.result.intValue()}, new DataType[]{DataType.INT});
		}

		return finalTuple;

	}

	@Override
	public void close() {
		child.close();
	}

}
