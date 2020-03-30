
package org.knime.core.data.store;

public interface VectorStore<T> {

	T createNextVectorForWriting();

	T getVectorForReading(int index);

	// TODO: delegates to central cache; loader, writer for persistence
}
