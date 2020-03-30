
package org.knime.core.data.store.arrow.table;

public interface VectorStore<T> {

	T getNextVectorForWriting();

	boolean hasVectorForReading(long index);

	T getVectorForReading(long index);
}
