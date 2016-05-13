package org.ctrl;

//import com.google.gson.*;
//import com.google.gson.JsonObject;
import org.dao.DAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xquery.XQException;
import java.io.IOException;

/**
 * Created by Stepan Danilov on 07.12.2015.
 */

@RestController
@EnableWebMvc

public class RestCtrl {
    @Autowired
    DAO dao;

    @RequestMapping(value = "query/{number}", method = RequestMethod.GET, produces = "application/json;charset=UTF-8")
    public Object query(@PathVariable int number, @RequestParam(value = "patient_id", required = false) Integer patient_id,
                        @RequestParam(value = "case_id", required = false) Integer case_id){

        switch (number){
            case 1: return dao.query1(patient_id);
            case 2: return dao.query2(patient_id);
            case 3: return dao.query3(case_id);

        }

        return null;
    }

    @RequestMapping(value = "temp4", method = RequestMethod.GET, produces = "application/json;charset=UTF-8")
    public String temp4() throws XQException, XMLStreamException, TransformerException, IOException {
        return dao.temp4();


    }

}
