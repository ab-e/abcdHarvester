package de.pangaea.abcdharvester.xml;

import java.io.FilterReader;
import java.io.IOException;
import java.io.Reader;
import java.util.regex.Pattern;

import de.pangaea.abcdharvester.util.FileIO;



public class InvalidXmlCharacterFilter extends FilterReader {

    protected InvalidXmlCharacterFilter(Reader in) {
        super(in);
    }
//
//    @Override
//    public int read(char[] cbuf, int off, int len) throws IOException {
//        int read = super.read(cbuf, off, len);
//        if (read == -1) return read;
//
//        for (int i = off; i < off + read; i++) {
//            if (!XMLChar.isValid(cbuf[i])) cbuf[i] = '?';
//        }
//        return read;
//    }
   // StringBuffer bu = FileIO.readFileForuffer("response.01472.xml");
    //Pattern p = Pattern.compile("(\\u0x1e)");
    
    //Pattern p = Pattern.compile("[^\\u0009\\u000A\\u000D\\0x1e\\u0020-\\uD7FF\\uE000-\\uFFFD\\u10000-\\u10FFF]");
    //returnContent = p.matcher(retunContent).replaceAll("");
}
