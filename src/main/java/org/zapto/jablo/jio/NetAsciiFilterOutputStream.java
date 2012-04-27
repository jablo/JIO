/*
 * $Id: NetAsciiFilterOutputStream.java,v 1.1.1.1 2008-09-04 21:43:12 jablo Exp $
 * $Name: not supported by cvs2svn $
 * 
 * Jacob Lorensen, TDC KabelTV, 2006
 */
package org.zapto.jablo.jio;

import java.io.*;

/**
 * NetAsciiFilterOutputStream is a FilterOutputStream that translates from
 * NETASCII to 8-bit ASCII standard.
 * 
 * The output file line env convention follows the definition of
 * System.getProperty("line.separator") and thus should follow the platform's
 * default line separator conventions.
 * <p>
 * 
 * The bytes are translated on-the-fly from NETASCII into native platform ASCII.
 * That is
 * <ol>
 * <li>On UNIX (LF) machines:
 * <ul>
 * <li>CR LF &rarr; LF</li>
 * <li>CR NUL &rarr; CR</li>
 * </ul>
 * Other combinations are passed as-is silently</li>
 * <li>On Windows, MS-DOS (CRLF) machines:
 * <ul>
 * <li>CR LF &rarr; CR LF </li>
 * <li>CR NUL &rarr; CR </li>
 * </ul>
 * Other combinations are passed as-is silently, including an signle terminating
 * CR</li>
 * </ul>
 * 
 * @author Jacob Lorensen, TDC KabelTV, 2006.
 */
public class NetAsciiFilterOutputStream extends FilterOutputStream {
    private final String lineEnd = System.getProperty("line.separator");
    private OutputFilter filter;

    /**
     * Create a NetAsciiFilterOutputStream.
     * 
     * @throws RuntimeException
     *                 if the platforms line end convention is not supported
     */
    public NetAsciiFilterOutputStream(OutputStream stream) {
	super(stream);
	if (lineEnd.equals("\r\n"))
	    filter = new DosFilter();
	else if (lineEnd.equals("\n"))
	    filter = new UnixFilter();
	else
	    throw new RuntimeException(
	    "Only Unix (lf) or MS-DOS (crlf) line end convention supported");
    }

    private abstract class OutputFilter {
	public abstract void write(byte b) throws IOException;

	public abstract void close() throws IOException;
    }

    /**
     * The NETASCII to Unix ASCII converter
     */
    private class UnixFilter extends OutputFilter {
	private int c1;

	/**
	 * Write an ASCII character to the output stream converting from
	 * NETASCIII to native ASCII representation.
	 */
	public void write(byte b) throws IOException {
	    if (c1 == 0) {
		if (b == 13)
		    c1 = b; // CR --- remember
		else
		    NetAsciiFilterOutputStream.super.write(b); // Others pass
		// through as is
	    } else if (c1 == 13) {
		if (b == 10) { // CR + LF --> LF
		    NetAsciiFilterOutputStream.super.write(b);
		    c1 = 0;
		} else if (b == 0) { // CR NUL --> CR
		    NetAsciiFilterOutputStream.super.write(c1);
		    c1 = 0;
		} else { // Others pass through as is
		    NetAsciiFilterOutputStream.super.write(c1);
		    NetAsciiFilterOutputStream.super.write(b);
		}
	    }
	}

	public void close() throws IOException {
	    if (c1 != 0)
		NetAsciiFilterOutputStream.super.write(c1);
	    NetAsciiFilterOutputStream.super.close();
	}
    }

    /**
     * The NETASCII to MS-DOS ASCII converter
     */
    private class DosFilter extends OutputFilter {
	private int c1;

	/**
	 * Write an ASCII character to the output stream converting from
	 * NETASCIII to native MS-DOS line end convention ASCII representation.
	 */
	public void write(byte b) throws IOException {
	    if (c1 == 0) {
		if (b == 13)
		    c1 = b; // CR --- remember
		else
		    NetAsciiFilterOutputStream.super.write(b); // Others pass
		// through as is
	    } else if (c1 == 13) {
		if (b == 10) { // CR + LF --> CR + LF
		    NetAsciiFilterOutputStream.super.write(c1);
		    NetAsciiFilterOutputStream.super.write(b);
		    c1 = 0;
		} else if (b == 0) { // CR NUL --> CR
		    NetAsciiFilterOutputStream.super.write(c1);
		    c1 = 0;
		} else { // Others pass through as is
		    NetAsciiFilterOutputStream.super.write(c1);
		    NetAsciiFilterOutputStream.super.write(b);
		}
	    }
	}

	public void close() throws IOException {
	    if (c1 != 0)
		NetAsciiFilterOutputStream.super.write(c1);
	    NetAsciiFilterOutputStream.super.close();
	}
    }

    public void write(byte b) throws IOException {
	filter.write(b);
    }

    public void write(byte[] b) throws IOException {
	int i;
	for (i = 0; i < b.length; i++)
	    write(b[i]);
    }
}
