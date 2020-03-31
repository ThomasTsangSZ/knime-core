
package org.knime.core.data.store.arrow.table;

public interface VectorStore<T> {

	T getNextVectorForWriting();

	void returnLastWritteOnVector(T vector);

	boolean hasVectorForReading(long index);

	T getVectorForReading(long index);

	void returnReadFromVector(long index, T vector);
}
