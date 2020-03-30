
package org.knime.core.data.store.table.column;

public interface ReadableColumn extends AutoCloseable {

	long size();

	ReadableColumnIterator iterator();

}
