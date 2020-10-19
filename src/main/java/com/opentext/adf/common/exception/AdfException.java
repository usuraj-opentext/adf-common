package com.opentext.adf.common.exception;

/**
 * Exception class defined for the ADF Application.
 *
 * @author opentext
 */
public class AdfException extends RuntimeException {
  private static final String EMPTY = "";
  private final String code;

  public AdfException(String msg) {
    super(msg);
    code = EMPTY;
  }

  public AdfException(String code, String msg) {
    super(msg);
    this.code = code;
  }

  public AdfException(String msg, Throwable th) {
    super(msg, th);
    this.code = EMPTY;
  }

  public AdfException(String code, String msg, Throwable th) {
    super(msg, th);
    this.code = code;
  }

  public String getCode() {
    return code;
  }
}
