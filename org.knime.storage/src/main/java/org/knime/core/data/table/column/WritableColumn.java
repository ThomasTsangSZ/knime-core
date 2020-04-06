package org.knime.core.data.table.column;

public interface WritableColumn {
	/**
	 * @return a new cursor over the column. Must be closed when done.
	 */
	WritableColumnCursor createWritableCursor();

}
