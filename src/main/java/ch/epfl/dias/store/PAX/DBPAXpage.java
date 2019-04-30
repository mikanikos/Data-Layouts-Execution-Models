package ch.epfl.dias.store.PAX;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class DBPAXpage {

    // fields represent the set of tuples in the page
    public DBTuple[] fields;
    public boolean eof;

    public DBPAXpage(DBTuple[] fields) {
        this.fields = fields;
        this.eof = false;
    }

    public DBPAXpage() {
        this.eof = true;
    }

    /**
     * XXX Assuming that the caller has ALREADY checked the datatype, and has
     * made the right call
     *
     * @param fieldNo
     *            (starting from 0)
     * @return cast of field
     */

    public DBTuple getDBTuple(int fieldNo) {
        return fields[fieldNo];
    }

}
