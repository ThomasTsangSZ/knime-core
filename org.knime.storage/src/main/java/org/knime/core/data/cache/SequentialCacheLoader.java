
package org.knime.core.data.cache;

import java.io.IOException;

import org.knime.core.data.table.column.Partition;

public interface SequentialCacheLoader<O> extends AutoCloseable {

	Partition<O> load(long index) throws IOException;
}
