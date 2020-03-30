
package org.knime.core.data.store.table.row;

/**
 * Base interface for proxies through which data values are read.
 */
public interface ReadableValueAccess {

	boolean isMissing();
}
