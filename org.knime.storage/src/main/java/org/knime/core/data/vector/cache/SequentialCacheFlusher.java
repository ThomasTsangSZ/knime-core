
package org.knime.core.data.vector.cache;

import java.io.IOException;

public interface SequentialCacheFlusher<O extends AutoCloseable> {

	void flush(O obj) throws IOException;
}
