package org.knime.core.data.store.table;

import java.util.Iterator;

public interface ChunkedTable extends Iterator<Row> {

	// tables can be destroyed. no trace left.
	public void destroy();
}
