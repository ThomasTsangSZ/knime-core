
package org.knime.core.data.inmemory.array;

public class NativeDoubleArrayFactory extends AbstractNativeArrayFactory<NativeDoubleArray> {

	public NativeDoubleArrayFactory(final int partitionCapacity) {
		super(partitionCapacity);
	}

	@Override
	NativeDoubleArray create(final int capacity) {
		return new NativeDoubleArray(capacity);
	}
}
