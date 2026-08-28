/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.lakehouse.delta;

import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.NoSuchElementException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CloseableIterators {

    public static <T> CloseableIterator<T> empty() {
        return new CloseableIterator<T>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public T next() {
                throw new NoSuchElementException();
            }

            @Override
            public void close() {
            }
        };
    }

    public static <T> CloseableIterator<T> of(T... elements) {
        if (elements == null || elements.length == 0) {
            return empty();
        }
        return new ArrayCloseableIterator<>(elements);
    }

    public static <T> CloseableIterator<T> singleton(T element) {
        return of(element);
    }

    private static class ArrayCloseableIterator<T> implements CloseableIterator<T> {
        private final T[] elements;
        private int index = 0;
        private boolean closed = false;

        @SafeVarargs
        ArrayCloseableIterator(T... elements) {
            this.elements = elements;
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            return index < elements.length;
        }

        @Override
        public T next() {
            if (closed || index >= elements.length) {
                throw new NoSuchElementException();
            }
            return elements[index++];
        }

        @Override
        public void close() {
            closed = true;
            for (T element : elements) {
                if (element instanceof AutoCloseable) {
                    try {
                        ((AutoCloseable) element).close();
                    } catch (Exception e) {
                        log.debug("Failed to close resource in iterator", e);
                    }
                }
            }
        }
    }

    public static class CountingCloseableIterator<T> implements CloseableIterator<T> {
        private final CloseableIterator<T> delegate;
        private int nextCount = 0;

        public CountingCloseableIterator(CloseableIterator<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public T next() {
            nextCount++;
            return delegate.next();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        public int getNextCount() {
            return nextCount;
        }

        public void resetCount() {
            nextCount = 0;
        }
    }
}