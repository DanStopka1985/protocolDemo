package org.utils;

import cz.atria.lsd.md.ehr.xmldb.complex.ComplexXmlConnection;

import java.util.List;

/**
 * Created by Stepan Danilov on 13.05.2016.
 */
public class Util {
    public String getDocumentWithPath(String path, ComplexXmlConnection conn){
        return "<doc>" + "<path>" + path + "</path>" + conn.queryXml(conn.getDocument(path), "let $i := :in/version/data return $i" ) + "</doc>";
    }

    public String getDocumentsWithPaths(List<String> paths, ComplexXmlConnection conn){
        String r = "";
        for (String path : paths) {
            r += getDocumentWithPath(path, conn);
        }
        return "<docs>" + r + "</docs>";
    }

}
