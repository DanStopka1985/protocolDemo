package org.ctrl;

import org.dao.DAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import javax.xml.stream.XMLStreamException;
import javax.xml.transform.TransformerException;
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
    public Object query(@PathVariable String number,
                        @RequestParam(value = "patient_id", required = false) Integer patient_id,
                        @RequestParam(value = "case_id", required = false) Integer case_id) throws IOException {

        switch (number){
            case "1": return dao.query1(patient_id);
            case "2": return dao.query2(patient_id);
            case "3": return dao.query3(case_id);
            case "4": return dao.query4(case_id);
            case "5": return dao.query5(case_id);
            case "6": return dao.query6(case_id);

            case "8": return dao.query8(case_id);
            case "9": return dao.query9(case_id);
            case "10": return dao.query10(case_id);

        }

        return null;
    }

//    @RequestMapping(value = "temp4", method = RequestMethod.GET, produces = "application/json;charset=UTF-8")
//    public String temp4() throws XQException, XMLStreamException, TransformerException, IOException {
//        return dao.temp4();
//
//
//    }

}
