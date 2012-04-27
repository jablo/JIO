/*
 * $Id: NetAsciiFilterInputStream.java,v 1.1.1.1 2008-09-04 21:43:12 jablo Exp $
 * $Name: not supported by cvs2svn $
 * 
 * Jacob Lorensen, TDC KabelTV, 2006
 */
package local.lorensen.jacob.jio;

import java.io.*;

/**
 * NetAsciiFilterInputStream is a FilterInputStream that translates from ASCII
 * to NETASCII standard.
 * 
 * The input file is assumed to be a stream of bytes in ASCII with either Unix
 * (\n) or MS-DOS (\r\n) line end convention. The line end convention is assumed
 * to be consistent throughout the file.
 * <p>
 * 
 * The bytes are translated on-the-fly into NETASCII transfer mode. That is:
 * <ul>
 * <li>Carriage Return is converted to Carriage Return followed by a NUL
 * character
 * <li>Line feed is converted to a Carriage Return followed by Line feed
 * </ul>
 * 
 * @author Jacob Lorensen, TDC KabelTV, 2006.
 */
public class NetAsciiFilterInputStream extends FilterInputStream {
    public NetAsciiFilterInputStream(InputStream stream) {
	super(stream);
	nextChar = -1;
	pushback = -2;
    }

    private int pushback; // to "unget" 1 character

    private int getc() throws IOException {
	if (pushback == -2)
	    return super.read();
	int ch = pushback;
	pushback = -2;
	return ch;
    }

    private void ungetc(int ch) {
	pushback = ch;
    }

    /**
     * nextChar, if not -1, holds the character to be injected in the input
     * stream on the next read()
     */
    private int nextChar;

    /**
     * Read an ASCII character from the input stream converting to NETASCII
     * representation.
     * 
     * This function handles both DOS style line end and Unix style line
     * ends, under the assumption that the file is consistent in use of line
     * ends.
     */
    public int read() throws IOException {
	// Insertion of extra nul or cr
	if (nextChar != -1) {
	    int c = nextChar;
	    nextChar = -1;
	    return c;
	}
	int c, c1;
	if ((c = getc()) == -1 || c == 10) {
	    return c;
	}
	c1 = getc();
	ungetc(c1); // lookahead
	if (c == 13) {
	    if (c1 != 10) { // cr + !lf --> cr + nul + !lf
		nextChar = 0;
	    }
	} else if (c1 == 10) { // !cr+lf --> !cr + cr + lf
	    nextChar = 13;
	}
	return c;
    }

    public int read(byte[] b) throws IOException {
	int i;
	for (i = 0; i < b.length; i++) {
	    int c = read();
	    if (c < 0)
		break;
	    b[i] = (byte) c;
	}
	return i;
    }
}
