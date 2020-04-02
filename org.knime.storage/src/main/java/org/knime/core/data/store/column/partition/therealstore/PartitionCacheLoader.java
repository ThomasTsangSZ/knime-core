
package org.knime.core.data.store.column.partition.therealstore;

import java.io.IOException;

public interface PartitionCacheLoader<P extends AutoCloseable> {

	P loadPartition(long index) throws IOException;
}
