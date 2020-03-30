
package org.knime.core.data.store;

public class ChunkedVectorStore<T> implements VectorStore<T> {

	@Override
	public T createNextVector() {
		throw new IllegalStateException("not yet implemented"); // TODO: implement

	}
}
