package io.puntanegra.fhir.index;

/**
 * {@code RuntimeException} to be thrown when there are Lucene {@link FhirIndex}
 * -related errors.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class FhirIndexException extends RuntimeException {

	private static final long serialVersionUID = 2532456234653465436L;

	/**
	 * Constructs a new index exception with the specified formatted detail
	 * message.
	 *
	 * @param message
	 *            the detail message.
	 * @param args
	 *            arguments referenced by the format specifiers in the format
	 *            message
	 */
	public FhirIndexException(String message, Object... args) {
		super(String.format(message, args));
	}

	/**
	 * Constructs a new index exception with the specified formatted detail
	 * message.
	 *
	 * @param cause
	 *            the cause
	 * @param message
	 *            the detail message
	 * @param args
	 *            arguments referenced by the format specifiers in the format
	 *            message
	 */
	public FhirIndexException(Throwable cause, String message, Object... args) {
		super(String.format(message, args), cause);
	}

	/**
	 * Constructs a new index exception with the specified cause.
	 *
	 * @param cause
	 *            the cause
	 */
	public FhirIndexException(Throwable cause) {
		super(cause);
	}

}