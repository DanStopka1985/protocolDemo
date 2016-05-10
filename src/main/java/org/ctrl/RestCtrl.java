package org.ctrl;

//import com.google.gson.*;
//import com.google.gson.JsonObject;
import org.dao.DAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import java.util.List;

/**
 * Created by Stepan Danilov on 07.12.2015.
 */

@RestController
@EnableWebMvc

public class RestCtrl {
    @Autowired
    DAO bookDAO;


    @RequestMapping(value = "query1/{id}", method = RequestMethod.GET, produces = "application/json;charset=UTF-8")
    public String getBookById(@PathVariable int id){
        return bookDAO.getById(id);
    }

    @RequestMapping(value = "temp", method = RequestMethod.GET, produces = "application/json")
    public String temp() {

        String jsonString = "{\n" +
                "  \"services\":[\n" +
                "    {\n" +
                "      \"d\":\"28.04.2016\",\n" +
                "      \"n\":\"УЗИ поясничного отдела позвоночника\",\n" +
                "      \"c\":\"Протрузия пояснично-крестцового отдела L3-L4\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"d\":\"28.04.2016\",\n" +
                "      \"n\":\"УЗИ брош.полости\",\n" +
                "      \"c\":\"лыфдлаовд длвыджлоадылоа длывдладвыладждл   длвдвлывдаыажда\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

//      JsonObject obj = new JsonParser().parse(jsonString).getAsJsonObject();
//      return obj.toString


        return jsonString;
    }
}
