package org.knime.core.data.store.vec.rw;

import org.knime.core.data.store.vec.VecAccess;

public interface VecReadAccess extends VecAccess {

	boolean isMissing();

}
