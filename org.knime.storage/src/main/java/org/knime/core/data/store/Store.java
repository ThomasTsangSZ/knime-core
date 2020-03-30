package org.knime.core.data.store;

import org.knime.core.data.store.table.column.WritableColumn;

public interface Store {

	long getNumLogicalColumns();

	WritableColumn getLogicalColumnAt(long index);

	void closeForWriting();

}
