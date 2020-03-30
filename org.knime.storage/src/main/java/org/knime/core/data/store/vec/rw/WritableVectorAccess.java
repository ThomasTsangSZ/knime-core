
package org.knime.core.data.store.vec.rw;

public interface WritableVectorAccess extends AutoCloseable {

	void fwd();

	void setMissing();
}
