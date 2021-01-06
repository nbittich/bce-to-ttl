package tech.artcoded.csvtottl.utils;

import java.util.function.Supplier;

@FunctionalInterface
public interface CheckedSupplier<T> {
  static <F> Supplier<F> toSupplier(CheckedSupplier<F> hack) {
    return hack::safeGet;
  }

  T get() throws Exception;

  default T safeGet() {
    try {
      return get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}