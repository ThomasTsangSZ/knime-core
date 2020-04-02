
package org.knime.core.data.store.column.partition.therealstore;

import java.io.IOException;

public interface PartitionCacheFlusher<P extends AutoCloseable> {

	void flushPartition(P partition) throws IOException;
}
