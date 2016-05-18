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
                        @RequestParam(value = "case_id", required = false) Integer case_id,
                        @RequestParam(value = "step_id", required = false) Integer step_id,
                        @RequestParam(value = "epicrisis_type_id", required = false) Integer epicrisis_type_id) {

        switch (number){
            case "1": return dao.query1(patient_id);
            case "2": return dao.query2(patient_id);
            case "3": return dao.query3(case_id);
            case "4": return dao.query4(case_id);
            case "5": return dao.query5(case_id);
            case "6": return dao.query6(case_id);
            case "7": return dao.query7(case_id); //query/7?case_id=885652
            case "8": return dao.query8(case_id);
            case "9": return dao.query9(case_id); //http://localhost:8080/query/9?case_id=885729
            case "10": return dao.query10(case_id);
            case "11": return dao.query11(case_id); //http://localhost:8080/query/11?case_id=885652
            case "12": return dao.query12(case_id);
            case "13a": return dao.query13a(case_id); //http://localhost:8080/query/13a?case_id=885652
            case "13b": return dao.query13b(case_id); //http://localhost:8080/query/13b?case_id=885652
            case "14a": return dao.query14a(case_id); //http://localhost:8080/query/14a?case_id=885652
            case "14b": return dao.query14b(case_id); //http://localhost:8080/query/14b?case_id=885652
            case "16": return dao.query16(step_id); //http://localhost:8080/query/16?step_id=698541
            case "17": return dao.query17(case_id); //http://localhost:8080/query/17?case_id=189
            case "18": return dao.query18(case_id); //http://localhost:8080/query/18?case_id=189
            case "20": return dao.query20(case_id);
            case "21": return dao.query21(case_id);
            case "22": return dao.query22(case_id);
            case "24": return dao.query24(case_id, epicrisis_type_id);
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
