package org.ctrl;

import com.google.gson.*;
import com.google.gson.JsonObject;
import org.dao.BookDAO;
import org.entities.A;
import org.entities.Book;
//import org.json.JSONArray;
//import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.expression.ParseException;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import java.io.StringReader;
import java.util.List;
import javax.json.*;

/**
 * Created by Stepan Danilov on 07.12.2015.
 */

@RestController
@EnableWebMvc

public class RestCtrl {
    @Autowired
    BookDAO bookDAO;


    @RequestMapping(value = "book/{id}", method = RequestMethod.GET)
    public List<A> getBookById(@PathVariable int id){
        return bookDAO.getById(1);
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

//        String jsonString = "{\"d\":\"28.04.2016\"}";


//        JsonObject obj = new JsonParser().parse(jsonString).getAsJsonObject();

//        JSONArray jsonArray = new JSONArray(jsonString);
//        JSONObject jsnobject = new JSONObject(jsonString);

//        JSONArray jsonArray = jsnobject.getJSONArray("services");
//        for (int i = 0; i < jsonArray.length(); i++) {
//            JSONObject explrObject = jsonArray.getJSONObject(i);
//        }

//        Gson gson = new Gson();
//        JsonParser jsonParser = new JsonParser();

//        JsonArray body = Json.createReader(new StringReader(jsonString)).readObject();
        return jsonString;
    }
}
