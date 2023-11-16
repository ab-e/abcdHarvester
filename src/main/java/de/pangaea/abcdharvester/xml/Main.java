package de.pangaea.abcdharvester.xml;
import java.io.OutputStream;
import java.io.StringReader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.DocumentType;
import org.xml.sax.InputSource;

public class Main {
  
  public static void main(String[] argv) throws Exception {
    StringReader sr = new StringReader("<tag>müller</tag>");
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder loader = factory.newDocumentBuilder();

    Document document = loader.parse(new InputSource(sr));
    
    writeXml(document, System.out);

  }
  public static void writeXml(Document doc, OutputStream out) throws Exception {
    Transformer t = TransformerFactory.newInstance().newTransformer();
    DocumentType dt = doc.getDoctype();
    if (dt != null) {
      String pub = dt.getPublicId();
      if (pub != null) {
        t.setOutputProperty(OutputKeys.DOCTYPE_PUBLIC, pub);
      }
      t.setOutputProperty(OutputKeys.DOCTYPE_SYSTEM, dt.getSystemId());
    }
    t.setOutputProperty(OutputKeys.ENCODING, "UTF8"); // NOI18N
    t.setOutputProperty(OutputKeys.INDENT, "yes"); // NOI18N
    t.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4"); // NOI18N
    Source source = new DOMSource(doc);
    Result result = new StreamResult(out);
    t.transform(source, result);
}
}