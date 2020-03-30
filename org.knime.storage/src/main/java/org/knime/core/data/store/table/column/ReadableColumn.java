package org.knime.core.data.store.table.column;

public interface ReadableColumn {

	long size();

	ReadableColumnIterator iterator();

}
