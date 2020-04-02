
package org.knime.core.data.table.column;

public interface ReadableColumn {

	/**
	 * @return a new cursor over the column. Must be closed when done.
	 */
	ReadableColumnCursor createCursor();
}
