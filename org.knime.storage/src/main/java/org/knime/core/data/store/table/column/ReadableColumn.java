
package org.knime.core.data.store.table.column;

public interface ReadableColumn {

	// TODO Naming: 'ReadableColumnCursor'?
	ReadableColumnCursor iterator();
}
