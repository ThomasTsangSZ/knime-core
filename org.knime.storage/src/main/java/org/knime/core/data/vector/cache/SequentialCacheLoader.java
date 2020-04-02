
package org.knime.core.data.vector.cache;

import java.io.IOException;

public interface SequentialCacheLoader<O extends AutoCloseable> {

	O load(long index) throws IOException;
}
