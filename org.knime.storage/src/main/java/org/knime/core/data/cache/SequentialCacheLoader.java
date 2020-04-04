
package org.knime.core.data.cache;

import java.io.IOException;

public interface SequentialCacheLoader<O> extends AutoCloseable {

	RefCountingPartition<O> load(long index) throws IOException;
}
