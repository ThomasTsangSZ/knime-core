
package org.knime.core.data.cache;

import java.io.IOException;

import org.knime.core.data.table.column.Partition;

public interface SequentialCacheFlusher<O> extends AutoCloseable {

	void flush(Partition<O> obj) throws IOException;
}
