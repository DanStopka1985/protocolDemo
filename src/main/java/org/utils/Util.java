package org.utils;

import cz.atria.lsd.md.ehr.xmldb.complex.ComplexXmlConnection;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;

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

    public String xmlToJSON(String xmlString){
        String jsonPrettyPrintString = null;
        int PRETTY_PRINT_INDENT_FACTOR = 4;
        try {
            JSONObject xmlJSONObj = XML.toJSONObject(xmlString);
            jsonPrettyPrintString = xmlJSONObj.toString(PRETTY_PRINT_INDENT_FACTOR);
        } catch (JSONException je) {
            System.out.println(je.toString());
        }
        return jsonPrettyPrintString;
    }

}
