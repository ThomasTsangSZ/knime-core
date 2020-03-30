
package org.knime.core.data.store.table.row;

/**
 * Base interface for proxies through which data values are written.
 */
public interface WritableValueAccess {

	void setMissing();
}
