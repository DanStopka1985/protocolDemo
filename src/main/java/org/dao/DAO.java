package org.dao;

import net.xqj.basex.BaseXXQDataSource;
import org.basex.api.client.ClientQuery;
import org.basex.api.client.ClientSession;
import org.basex.api.client.Session;
import org.basex.core.Context;
import org.basex.query.QueryProcessor;
import org.hibernate.SessionFactory;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

import org.basex.*;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.*;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xquery.*;

@Service
@SuppressWarnings(value = "unchecked")
public class DAO {
    @Autowired
    private SessionFactory sessionFactory;

    @Autowired
    XQDataSource baseXDataSource;

    @Transactional(readOnly = true)
    public String query1(Integer id){
        if (id == null) return "\"error\":\"patient_id - required parameter\"";
        List<String> list = sessionFactory.getCurrentSession().createSQLQuery("with t as (\n" +
                "select \n" +
                " (select \n" +
                "   concat(pot.name,': ', lower(po.short_name), case when ppj.is_main_job is true then ' (основное) ,' else ', ' end, coalesce(pd.name,ppj.study_group),\n" +
                "          case when ppj.from_dt is not null then  ' с ' || to_char(ppj.from_dt, 'dd.mm.yyyy') end, case when ppj.to_dt is not null then ' по '|| to_char(ppj.to_dt,'dd.mm.yyyy') end)\n" +
                "  from pci_patient_job ppj \n" +
                "  join pci_organization_type pot on pot.id = ppj.organization_type_id\n" +
                "  left join pim_okved ps on ps.id = ppj.okved_id\n" +
                "  join pim_organization po on po.id = ppj.organization_id\n" +
                "  left join pim_department pd on pd.id = ppj.department_id\n" +
                "  left join pim_profession_working ppw on ppw.id = ppj.profession_working_id\n" +
                "\n" +
                " where ppj.patient_id = i.id and current_date between coalesce(ppj.from_dt, '-infinity') and coalesce(ppj.to_dt, 'infinity') order by ppj.from_dt desc limit 1) ppj,\n" +
                "\n" +
                " trim(concat(trim(concat(surname, ' ', name)), ' ', patr_name)) fio,\n" +
                " birth_dt,\n" +
                " case when birth_dt is not null then\n" +
                "  extract(year from age(current_date, i.birth_dt))\n" +
                " end full_years,\n" +
                "case when birth_dt is not null then\n" +
                "  extract(month from age(current_date, i.birth_dt))\n" +
                " end full_months\n" +
                " \n" +
                " \n" +
                "from pim_individual i where i.id = :patient_id\n" +
                ")\n" +
                "\n" +
                "select \n" +
                " concat(fio, \n" +
                "  '; Возраст ' || \n" +
                "  case when birth_dt is not null then\n" +
                "    case when full_years < 18 then full_years || ' л.' || \n" +
                "      case when full_months != 0 then full_months || ' мес.' else '' end\n" +
                "    else full_years || ' л.' end\n" +
                "  else \n" +
                "    'не указан'   \n" +
                "  end,\n" +
                "  '; ', ppj\n" +
                "  ) val\n" +
                " \n" +
                "from t")
                .setParameter("patient_id", id/*5568540*/)
                .list();

        return list.isEmpty() ? null : list.get(0);

    }

    public String temp() throws XQException, XMLStreamException, TransformerException, IOException {
        XQConnection conn = baseXDataSource.getConnection();
        XQPreparedExpression expr = conn.prepareExpression
                ("/version[//id = 'c2e9bb01-acc1-4a64-b39b-f30ef514c5ec']//data[@archetype_node_id=\"at0000\"]/name/value");

        XQSequence result1 = expr.executeQuery();
        result1.next();

        XMLStreamReader result = result1.getSequenceAsStream();
//        Source resultXML = new StAXSource(result);
//        StreamResult resultSR = new StreamResult(System.out);

        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        StringWriter stringWriter = new StringWriter();
        transformer.transform(new StAXSource(result), new StreamResult(stringWriter));
        stringWriter.toString();






        return stringWriter.toString();

    }

    public String temp2() throws XQException, TransformerException, IOException {
        XQConnection conn = baseXDataSource.getConnection();
        XQPreparedExpression expr = conn.prepareExpression
                ("let $in_xml :=\n" +
                        "<ss><s><d>28.04.2016</d><n>УЗИ поясничного отдела позвоночника</n><p>2016/4/28/13/protocol13909.xml</p></s><s><d>28.04.2016</d><n>Операция на желудок</n><p>2016/4/28/10/protocol13908.xml</p></s><s><d>28.04.2016</d><n>УЗИ брош.полости</n><p>2016/4/28/10/protocol13907.xml</p></s><s><d></d><n>Прием (осмотр, консультация) врача - ортопеда повторный</n><p>2016/4/28/10/protocol13906.xml</p></s><s><d>28.04.2016</d><n>Прием (осмотр, консультация) врача - психотерапевта первичный</n><p>2016/4/28/10/protocol13905.xml</p></s><s><d>28.04.2016</d><n>Эхокардиография </n><p>2016/4/28/10/protocol13904.xml</p></s><s><d>28.04.2016</d><n>Тестовая ХР ОАК</n><p>2016/4/28/10/protocol13902.xml</p></s><s><d>28.04.2016</d><n>Осмотр оториноларинголога для справки в ГИБДД</n><p>2016/4/28/10/protocol13901.xml</p></s></ss>\n" +
                        " \n" +
                        "let $r := <json>\n" +
                        "{for $i in $in_xml//s,\n" +
                        "    $j in /version\n" +
                        "where \n" +
                        " $j/fn:base-uri() = $i/p and \n" +
                        " $j/data/content[.//value=\"openEHR-EHR-OBSERVATION.zakluchenie.v1\"]\n" +
                        "\n" +
                        "return <services>{$i/d} {$i/n} <c>{$j/data/content[.//value=\"openEHR-EHR-OBSERVATION.zakluchenie.v1\"]/\n" +
                        "        data[@archetype_node_id=\"at0001\"]/events[@archetype_node_id=\"at0002\"]/data[@archetype_node_id=\"at0003\"]/\n" +
                        "        items[@archetype_node_id=\"at0004\"]/value/value/text()}</c></services>}\n" +
                        "</json>\n" +
                        "\n" +
                        "return $r");

        XQSequence result1 = expr.executeQuery();
        result1.next();
        XMLStreamReader result = result1.getSequenceAsStream();
        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        StringWriter stringWriter = new StringWriter();
        transformer.transform(new StAXSource(result), new StreamResult(stringWriter));
        String xmlString = stringWriter.toString();

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
