
package org.knime.core.data.store.column;

public interface ReadableColumn {

	/**
	 * @return a new cursor over the column. Must be closed when done.
	 */
	ReadableColumnCursor cursor();
}
