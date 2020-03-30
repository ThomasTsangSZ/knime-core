
package org.knime.core.data.store.vec.rw;

import java.util.NoSuchElementException;

public interface ReadableVectorAccess extends AutoCloseable {

	boolean canFwd();

	/**
	 * @throws NoSuchElementException
	 */
	void fwd();

	boolean isMissing();
}
