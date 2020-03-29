
package org.knime.core.data.store.table.column;

import java.util.NoSuchElementException;

public interface ReadableColumn extends AutoCloseable {

	boolean canFwd();

	/**
	 * @throws NoSuchElementException
	 */
	void fwd();

	boolean isMissing();
}
