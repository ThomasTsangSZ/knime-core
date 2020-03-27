package org.knime.core.data.container.newapi.store;

public interface StoreReadAccessConfig {

    // TODO: Add predicates?

    long getStartIndex();

    long getEndIndex();

    long[] getColumnIndices();
}