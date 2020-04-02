package org.knime.core.data.arrow;

import org.knime.core.data.store.Store;

public interface ArrowStore<F> extends Store<F> {
	void flush() throws Exception;
}
