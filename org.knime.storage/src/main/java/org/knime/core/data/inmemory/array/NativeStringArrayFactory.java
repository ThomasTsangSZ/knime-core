
package org.knime.core.data.inmemory.array;

public final class NativeStringArrayFactory extends AbstractNativeArrayFactory<NativeStringArray> {

	public NativeStringArrayFactory(final int partitionCapacity) {
		super(partitionCapacity);
	}

	@Override
	NativeStringArray create(final int capacity) {
		return new NativeStringArray(capacity);
	}
}
