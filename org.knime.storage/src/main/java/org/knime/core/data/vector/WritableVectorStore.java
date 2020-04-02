
package org.knime.core.data.vector;

import org.apache.commons.lang3.tuple.Pair;

// TODO would it help us to make use of the sequential character of the data?
// TODO alternative name: partitioned vector
public interface WritableVectorStore<D> extends VectorStore<D> {

	/**
	 * @return a <Pair<K, Vector<D>> managed by the store.
	 */
	Pair<Long, Vector<D>> add();

}
