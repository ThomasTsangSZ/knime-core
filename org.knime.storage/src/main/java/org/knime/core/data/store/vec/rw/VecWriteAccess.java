
package org.knime.core.data.store.vec.rw;

public interface VecWriteAccess extends AutoCloseable {

	void fwd();

	void setMissing();
}
