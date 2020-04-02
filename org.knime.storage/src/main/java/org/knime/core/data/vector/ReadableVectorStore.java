
package org.knime.core.data.vector;

// TODO would it help us to make use of the sequential character of the data?
// TODO alternative name: partitioned vector
public interface ReadableVectorStore<D> extends VectorStore<D> {

	/**
	 * @return a <O> managed by the store. Either a new object or an existing one.
	 *         Increases ref counter before return.
	 */
	Vector<D> get(long idx);

	/**
	 * @return number of vectors in this store
	 */
	long numVectors();

}
