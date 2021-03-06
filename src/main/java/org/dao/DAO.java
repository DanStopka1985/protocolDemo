package org.dao;

import cz.atria.lsd.md.ehr.xmldb.complex.ComplexXmlConnection;
import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

import cz.atria.common.xmldb.XMLDataSource;
import cz.atria.ehr.templatestorage.impl.FilledProtocolStorage;
import org.utils.Util;

@Service
@SuppressWarnings(value = "unchecked")
public class DAO {
    @Autowired
    String repoPath;

    @Autowired
    XMLDataSource readOnlyXmlDataSource;

    @Autowired
    BasicDataSource dataSource;

    public String query1(Integer patientId){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String r = "";
        SqlRowSet rs =
        jdbcTemplate.queryForRowSet("with t as (\n" +
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
                "from pim_individual i where i.id = ? /*:patient_id*/\n" +
                ")\n" +
                "\n" +
                "select \n" +
                " concat('\"pat_name\": \"', fio, \n" +
                "  '\",\"pat_birth_dt\":\"' || \n" +
                "  case when birth_dt is not null then\n" +
                "    case when full_years < 18 then full_years || ' л. ' || \n" +
                "      case when full_months != 0 then full_months || ' мес. ' else '' end\n" +
                "    else full_years || ' л.' end \n" +
                "  else \n" +
                "    'не указан'   \n" +
                "  end, '\", \"pat_job\":\"', ppj, '\"'\n" +
                "  ) val\n" +
                " \n" +
                "from t limit 1", patientId);

        while (rs.next()) r = rs.getString("val");
        return r;
    }

    public String query2(Integer patientId){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String r = "";
        SqlRowSet rs =
                jdbcTemplate.queryForRowSet("with t as (\n" +
                        " select \n" +
                        "  (select ae.id\n" +
                        "   from pim_party_addr_to_addr_type ppatat\n" +
                        "   join pim_party_address ppa on ppa.id = ppatat.party_address_id\n" +
                        "   left join pim_register_type prt on prt.id = ppa.register_type_id\n" +
                        "   left join address_element ae on ae.id = ppa.addr_id\n" +
                        "   join pim_address_type padt on padt.id = ppatat.address_type_id and padt.party_type_id = 1\n" +
                        "   and upper(trim(padt.code)) in ('ACTUAL') and ppa.party_id = i.id) actual_id,\n" +
                        "  (select ae.id\n" +
                        "   from pim_party_addr_to_addr_type ppatat\n" +
                        "   join pim_party_address ppa on ppa.id = ppatat.party_address_id\n" +
                        "   left join pim_register_type prt on prt.id = ppa.register_type_id\n" +
                        "   left join address_element ae on ae.id = ppa.addr_id\n" +
                        "   join pim_address_type padt on padt.id = ppatat.address_type_id and padt.party_type_id = 1\n" +
                        "   and upper(trim(padt.code)) in ('REGISTER') and ppa.party_id = i.id) register_id,\n" +
                        "  (select \n" +
                        "   concat(dt.name, ': ', id.series, ' ', id.number, ' ', coalesce(org.short_name, ''), case when id.issue_dt is not null then ' с ' || to_char(id.issue_dt,'dd.mm.yyyy') else '' end,\n" +
                        "          case when id.expire_dt is not null then ' по ' || to_char(id.expire_dt,'dd.mm.yyyy') else '' end) \n" +
                        "  from pim_individual_doc id \n" +
                        "  join pim_doc_type dt on dt.id = id.type_id\n" +
                        "  join pim_doc_type_category dtc on dtc.type_id = dt.id and dtc.category_id = 1\n" +
                        "  left join pim_organization org on org.id = id.issuer_id \n" +
                        "  where id.indiv_id = i.id and id.is_active and current_date between coalesce(id.issue_dt, '-infinity') and coalesce(id.expire_dt, 'infinity') limit 1) doc\n" +
                        " from pim_individual i where i.id = ? /*patient.id*/\n" +
                        ")\n" +
                        "\n" +
                        "select \n" +
                        "concat('Документ: ' || doc, \n" +
                        " case when doc is not null then '; ' \n" +
                        " else '' end ||\n" +
                        " case when actual_id = register_id then adr__get_element_as_text(actual_id, '(2,s,0)(3,s,0)(4,s,0)(5,s,0)(6,s,0)(7,s,0)(8,s,0)(9,s,0)')\n" +
                        "      else 'Адрес прописки: ' || adr__get_element_as_text(actual_id, '(2,s,0)(3,s,0)(4,s,0)(5,s,0)(6,s,0)(7,s,0)(8,s,0)(9,s,0)') || '; Адрес регистрации: ' || adr__get_element_as_text(register_id, '(2,s,0)(3,s,0)(4,s,0)(5,s,0)(6,s,0)(7,s,0)(8,s,0)(9,s,0)')\n" +
                        " end\n" +
                        " ) val\n" +
                        "\n" +
                        "from t", patientId);

        while (rs.next()) r = rs.getString("val");
        return r;
    }

    public String query3(Integer caseId){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String r = "";
        SqlRowSet rs =
                jdbcTemplate.queryForRowSet("with t as (\n" +
                        " select \n" +
                        "  concat(s.name || ' - ', d.code, ' (', d.name, ')' ) d\n" +
                        " from mc_diagnosis md \n" +
                        " join md_diagnosis d on d.id = md.diagnos_id\n" +
                        " join mc_stage s on s.id = md.stage_id\n" +
                        " left join mc_diagnosis_type dt on dt.id = md.type_id\n" +
                        " where md.case_id = ? /*624 :case_id*/\n" +
                        " order by s.stage_order, md.establishment_date, dt.id\n" +
                        ")\n" +
                        "\n" +
                        "select string_agg(d, chr(13)) val from t", caseId);

        while (rs.next()) r = rs.getString("val");
        return r;
    }

    public String query4(Integer caseId){
        FilledProtocolStorage storage = new FilledProtocolStorage();
        storage.setRoot(repoPath);
        ComplexXmlConnection conn = new ComplexXmlConnection();
        conn.setFileStorage(storage);
        conn.setReadOnlyXmlDataSource(readOnlyXmlDataSource);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        Util util = new Util();
        List<String> fileList = new ArrayList<>();
        String paramFromSqlDb = "";

        //getting protocolList
        SqlRowSet rs = jdbcTemplate.queryForRowSet("select\n" +
                " path \n" +
                "from md_srv_rendered mr\n" +
                "join sr_srv_rendered r on r.id = mr.id\n" +
                "join sr_service s on s.id = r.service_id \n" +
                "join sr_srv_type st on st.id = s.type_id \n" +
                "join md_srv_protocol sp on sp.srv_rendered_id = r.id\n" +
                "join md_ehr_protocol p on sp.protocol_id = p.id\n" +
                "where case_id = ?", caseId);
        while (rs.next())
            fileList.add(rs.getString("path"));

        String docs = util.getDocumentsWithPaths(fileList, conn);

        //getting paramFromSqlDb
        rs = jdbcTemplate.queryForRowSet("select \n" +
                        "'<services>' || string_agg(concat('<service><path>', p.path, '</path>', '<bdate>', to_char(r.bdate, 'yyyy-mm-dd'), '</bdate></service>'), '') || '</services>'  val\n" +
                        "from md_srv_rendered mr \n" +
                        "\n" +
                        "join sr_srv_rendered r on r.id = mr.id\n" +
                        "join md_srv_protocol sp on sp.srv_rendered_id = r.id\n" +
                        "join md_ehr_protocol p on sp.protocol_id = p.id \n" +
                        "where mr.case_id = ?\n", caseId);

        while (rs.next()) paramFromSqlDb = rs.getString("val");

        //result query
        String xQuery = "(let $i := :in/docs/doc \n" +
                "let $x := \n" +
                paramFromSqlDb +
                "for $j in $x//service \n" +
                " for $k in $i \n" +
                "  where $j/path = $k/path \n" +
                "  and $k/data/content[.//archetype_id='openEHR-EHR-OBSERVATION.anamnesis_morbi.v1']/data[@archetype_node_id='at0001']/events[@archetype_node_id='at0002']/data[@archetype_node_id='at0003']/items[@archetype_node_id='at0004']/value/value/text() != ''\n" +
                "   order by $j/bdate \n" +
                "return $k/data/content[.//archetype_id='openEHR-EHR-OBSERVATION.anamnesis_morbi.v1']/data[@archetype_node_id='at0001']/events[@archetype_node_id='at0002']/data[@archetype_node_id='at0003']/items[@archetype_node_id='at0004']/value/value/text())[position() = 1]";

        return conn.queryXml(docs, xQuery);
    }

    public String query5(Integer caseId) {
        FilledProtocolStorage storage = new FilledProtocolStorage();
        storage.setRoot(repoPath);
        ComplexXmlConnection conn = new ComplexXmlConnection();
        conn.setFileStorage(storage);
        conn.setReadOnlyXmlDataSource(readOnlyXmlDataSource);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        Util util = new Util();
        List<String> fileList = new ArrayList<>();
        String paramFromSqlDb = "";

        //getting protocolList
        SqlRowSet rs = jdbcTemplate.queryForRowSet("select\n" +
                " path \n" +
                "from md_srv_rendered mr \n" +
                "join sr_srv_rendered r on r.id = mr.id\n" +
                "join md_srv_protocol sp on sp.srv_rendered_id = r.id\n" +
                "join md_ehr_protocol p on sp.protocol_id = p.id \n" +
                "join sr_service s on s.id = r.service_id\n" +
                "where upper(s.name) in ('ДНЕВНИК ВРАЧА', 'ОСМОТР', 'ОСМОТР В ПРИЕМНОМ ОТДЕЛЕНИИ') and mr.case_id = ?", caseId);
        while (rs.next())
            fileList.add(rs.getString("path"));

        String docs = util.getDocumentsWithPaths(fileList, conn);

        //getting paramFromSqlDb
        rs = jdbcTemplate.queryForRowSet("select \n" +
                " '<services>' ||\n" +
                "\tstring_agg(\n" +
                "\t '<service>'\n" +
                "\t\t'<path>' || p.path || '</path>' ||\n" +
                "\t\t'<priority>' ||\n" +
                "\t\t\tcase upper(s.name)\n" +
                "\t\t\t  when 'ДНЕВНИК ВРАЧА' then 0\n" +
                "\t\t\t  when 'ОСМОТР' then 1\n" +
                "\t\t\t  when 'ОСМОТР В ПРИЕМНОМ ОТДЕЛЕНИИ' then 2\n" +
                "\t\t\t end || to_char(r.bdate, 'yyyy-mm-dd') || \n" +
                "\t\t'</priority>'\n" +
                "\t '</service>'       \n" +
                "\t, '') ||\n" +
                " '</services>'\t val\n" +
                " \n" +
                "from md_srv_rendered mr \n" +
                "join sr_srv_rendered r on r.id = mr.id\n" +
                "join md_srv_protocol sp on sp.srv_rendered_id = r.id\n" +
                "join md_ehr_protocol p on sp.protocol_id = p.id \n" +
                "join sr_service s on s.id = r.service_id\n" +
                "where upper(s.name) in ('ДНЕВНИК ВРАЧА', 'ОСМОТР', 'ОСМОТР В ПРИЕМНОМ ОТДЕЛЕНИИ') and mr.case_id = ?", caseId);

        while (rs.next()) paramFromSqlDb = rs.getString("val");

        //result query
        String xQuery = "(let $i := :in/docs/doc \n" +
                "let $x := \n" +
                paramFromSqlDb +
                "for $j in $x//service \n" +
                " for $k in $i \n" +
                "  where $j/path = $k/path \n" +
                "  and //content[.//archetype_id='openEHR-EHR-OBSERVATION.complaints.v1' and ./name/value='Жалобы больного']/data[@archetype_node_id='at0001']/events[@archetype_node_id='at0002']/data[@archetype_node_id='at0003']/items[@archetype_node_id='at0020']/value/value/text() != ''" +
                "   order by $j/priority \n" +
                "return $k//content[.//archetype_id='openEHR-EHR-OBSERVATION.complaints.v1' and ./name/value='Жалобы больного']/data[@archetype_node_id='at0001']/events[@archetype_node_id='at0002']/data[@archetype_node_id='at0003']/items[@archetype_node_id='at0020']/value/value/text())[position() = 1]";

        return conn.queryXml(docs, xQuery);
    }

    public String query6(Integer caseId){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String r = "";
        SqlRowSet rs =
                jdbcTemplate.queryForRowSet("select \n" +
                        " string_agg(to_char(r.bdate, 'dd.mm.yyyy') || ' ' || '' || s.name, chr(13)) val\n" +
                        "from md_srv_rendered mr\n" +
                        "join sr_srv_rendered r on r.id = mr.id\n" +
                        "join sr_service s on s.id = r.service_id\n" +
                        "join sr_srv_type st on st.id = s.type_id and st.code = 'LABORATORY'\n" +
                        "where case_id = ? /*533148 :case_id*/", caseId);

        while (rs.next()) r = rs.getString("val");
        return r;
    }

    public String query7(Integer caseId){
        FilledProtocolStorage storage = new FilledProtocolStorage();
        storage.setRoot(repoPath);
        ComplexXmlConnection conn = new ComplexXmlConnection();
        conn.setFileStorage(storage);
        conn.setReadOnlyXmlDataSource(readOnlyXmlDataSource);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        Util util = new Util();
        List<String> fileList = new ArrayList<>();
        String paramFromSqlDb = "";

        //getting protocolList
        SqlRowSet rs = jdbcTemplate.queryForRowSet("select \n" +
                " p.path\n" +
                "from md_srv_rendered mr\n" +
                "join sr_srv_rendered r on r.id = mr.id\n" +
                "join sr_service s on s.id = r.service_id\n" +
                "join sr_srv_type st on st.id = s.type_id and st.code = 'LABORATORY'\n" +
                "join md_srv_protocol sp on sp.srv_rendered_id = r.id\n" +
                "join md_ehr_protocol p on sp.protocol_id = p.id \n" +
                "\n" +
                "where case_id = ? /*885652 :case_id*/", caseId);
        while (rs.next())
            fileList.add(rs.getString("path"));

        String docs = util.getDocumentsWithPaths(fileList, conn);

        //getting paramFromSqlDb
        rs = jdbcTemplate.queryForRowSet("with t as (\n" +
                "select \n" +
                "'<service>' ||\n" +
                "\t '<name>' ||\n" +
                "\t s.name ||\n" +
                "\t '</name>' ||\n" +
                "\t string_agg(\n" +
                "\t\t'<item><date>' || to_char(r.bdate, 'dd.mm.yyyy') || '</date>' || '<path>' || p.path || '</path></item>', \n" +
                "\t'') ||\n" +
                "'</service>' val\n" +
                "\n" +
                "from md_srv_rendered mr\n" +
                "join sr_srv_rendered r on r.id = mr.id\n" +
                "join sr_service s on s.id = r.service_id\n" +
                "join sr_srv_type st on st.id = s.type_id and st.code = 'LABORATORY'\n" +
                "join md_srv_protocol sp on sp.srv_rendered_id = r.id\n" +
                "join md_ehr_protocol p on sp.protocol_id = p.id \n" +
                "where case_id = ?\n" +
                "group by s.name\n" +
                ")\n" +
                "\n" +
                "select '<services>' || string_agg(val, '') || '</services>' val from t", caseId);

        while (rs.next()) paramFromSqlDb = rs.getString("val");

        //result query
        String xQuery = "<services>\n" +
                "{\n" +
                "let $i := :in/docs/doc \n" +
                "let $x := \n" +
                paramFromSqlDb +
                "for $j in $x/service/item\n" +
                "for $k in $i\n" +
                "where $j/path = $k/path\n" +
                "return <service>\n" +
                " {($j/../name, $j/date, \n" +
                " for $q in $k/data/content[./data/events/data/items/value/(magnitude, value)/text() != '']\n" +
                " return <param>\n" +
                "          <name>{$q/name/value/text()}</name>\n" +
                "          <unit>{$q/data/events/data/items/value/units/text()}</unit>\n" +
                "          <value>{$q/data/events/data/items/value/(magnitude, value)/text()}</value>\n" +
                "        </param>)}\n" +
                "</service>\n" +
                "}\n" +
                "</services>";

        String result = conn.queryXml(docs, xQuery);
        return util.xmlToJSON(result);
    }

    public String query8(Integer caseId){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String r = "";
        SqlRowSet rs =
                jdbcTemplate.queryForRowSet("\n" +
                        "select \n" +
                        " --'<services>' ||\n" +
                        "\tstring_agg('<service>' || to_char(r.bdate, 'dd.mm.yyyy') || ' ' || '' || s.name || '</service>', '') --||\n" +
                        " --'</services>' \n" +
                        " val\n" +
                        "from md_srv_rendered mr\n" +
                        "join sr_srv_rendered r on r.id = mr.id\n" +
                        "join sr_service s on s.id = r.service_id\n" +
                        "join sr_srv_type st on st.id = s.type_id and st.code = 'DIAGNOSTICS'\n" +
                        "where case_id = ? /*:case_id*/", caseId);

        while (rs.next()) r = rs.getString("val");
        Util util = new Util();
        return util.xmlToJSON(r);
    }

    public String query9(Integer caseId){
        FilledProtocolStorage storage = new FilledProtocolStorage();
        storage.setRoot(repoPath);
        ComplexXmlConnection conn = new ComplexXmlConnection();
        conn.setFileStorage(storage);
        conn.setReadOnlyXmlDataSource(readOnlyXmlDataSource);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        Util util = new Util();
        List<String> fileList = new ArrayList<>();
        String paramFromSqlDb = "";

        //getting protocolList
        SqlRowSet rs = jdbcTemplate.queryForRowSet("select\n" +
                " path \n" +
                "from md_srv_rendered mr\n" +
                "join sr_srv_rendered r on r.id = mr.id\n" +
                "join sr_service s on s.id = r.service_id \n" +
                "join sr_srv_type st on st.id = s.type_id --and st.code = 'DIAGNOSTICS'\n" +
                "join md_srv_protocol sp on sp.srv_rendered_id = r.id\n" +
                "join md_ehr_protocol p on sp.protocol_id = p.id\n" +
                "where case_id = ? /*885729*/", caseId);
        while (rs.next())
            fileList.add(rs.getString("path"));

        String docs = util.getDocumentsWithPaths(fileList, conn);

        //getting paramFromSqlDb
        rs = jdbcTemplate.queryForRowSet("select \n" +
                " concat('<ss>',\n" +
                "   string_agg(\n" +
                "     concat('<s>', '<date>', to_char(r.bdate, 'dd.mm.yyyy'), '</date>', '<name>', s.name, '</name>', '<p>', p.path, '</p>', '</s>'), ''\n" +
                "   ), '</ss>'\n" +
                " ) val\n" +
                "from md_srv_rendered mr\n" +
                "join sr_srv_rendered r on r.id = mr.id\n" +
                "join sr_service s on s.id = r.service_id \n" +
                "join sr_srv_type st on st.id = s.type_id --and st.code = 'DIAGNOSTICS'\n" +
                "join md_srv_protocol sp on sp.srv_rendered_id = r.id\n" +
                "join md_ehr_protocol p on sp.protocol_id = p.id\n" +
                "where case_id = ? /*885729*/ /*:case_id*/", caseId);
        while (rs.next()) paramFromSqlDb = rs.getString("val");

        String resultXML = conn.queryXml(docs, "let $i := :in/docs/doc \n" +
                "let $in_xml :=\n" +
                paramFromSqlDb +
                " let $r := <json>\n" +
                "{for $k in $in_xml//s,\n" +
                "    $j in $i\n" +
                "where $k/p = $j/path and $j/data/content[.//value=\"openEHR-EHR-OBSERVATION.zakluchenie.v1\"]\n" +
                "return <services>{$k/date} {$k/name} <conclusion>{$j/data/content[.//value=\"openEHR-EHR-OBSERVATION.zakluchenie.v1\"]/\n" +
                "        data[@archetype_node_id=\"at0001\"]/events[@archetype_node_id=\"at0002\"]/data[@archetype_node_id=\"at0003\"]/\n" +
                "        items[@archetype_node_id=\"at0004\"]/value/value/text()}</conclusion></services>}\n" +
                "</json> " +
                "return $r");

        return util.xmlToJSON(resultXML.substring(8, resultXML.length() - 7));//util.xmlToJSON(resultXML);
    }

    public String query10(Integer caseId){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String r = "";
        SqlRowSet rs =
                jdbcTemplate.queryForRowSet("select \n" +
                        " string_agg('<services><date>' || to_char(r.bdate, 'dd.mm.yyyy') || '</date>' || '<name>' || s.name || '</name></services>', '') val\n" +
                        "from md_srv_rendered mr\n" +
                        "join sr_srv_rendered r on r.id = mr.id\n" +
                        "join sr_service s on s.id = r.service_id\n" +
                        "join sr_srv_type st on st.id = s.type_id --and st.code = 'CONSULTATON'\n" +
                        "where case_id = ? /*:case_id*/", caseId);

        while (rs.next()) r = rs.getString("val");
        Util util = new Util();
        return util.xmlToJSON(r);
    }

    public String query11(Integer caseId){
        FilledProtocolStorage storage = new FilledProtocolStorage();
        storage.setRoot(repoPath);
        ComplexXmlConnection conn = new ComplexXmlConnection();
        conn.setFileStorage(storage);
        conn.setReadOnlyXmlDataSource(readOnlyXmlDataSource);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        Util util = new Util();
        List<String> fileList = new ArrayList<>();
        String paramFromSqlDb = "";

        //getting protocolList
        SqlRowSet rs = jdbcTemplate.queryForRowSet("select \n" +
                " p.path\n" +
                "from md_srv_rendered mr\n" +
                "join sr_srv_rendered r on r.id = mr.id\n" +
                "join md_srv_protocol sp on sp.srv_rendered_id = r.id\n" +
                "join md_ehr_protocol p on sp.protocol_id = p.id\n" +
                "join sr_service s on s.id = r.service_id\n" +
                "join sr_srv_type st on st.id = s.type_id and st.code in ('CONSULTATON', 'APPOINTMENT')\n" +
                "where case_id = ? /*:case_id*/", caseId);
        while (rs.next())
            fileList.add(rs.getString("path"));

        String docs = util.getDocumentsWithPaths(fileList, conn);

        //getting paramFromSqlDb
        rs = jdbcTemplate.queryForRowSet("select \n" +
                " '<services>' ||\n" +
                "\tstring_agg('<service><date>' || to_char(r.bdate, 'dd.mm.yyyy') || '</date>' || '<path>' || p.path || '</path>' || '<name>' || s.name || '</name></service>', '') ||\n" +
                "\t'</services>' val\n" +
                "from md_srv_rendered mr\n" +
                "join sr_srv_rendered r on r.id = mr.id\n" +
                "join md_srv_protocol sp on sp.srv_rendered_id = r.id\n" +
                "join md_ehr_protocol p on sp.protocol_id = p.id\n" +
                "join sr_service s on s.id = r.service_id\n" +
                "join sr_srv_type st on st.id = s.type_id and st.code in ('CONSULTATON', 'APPOINTMENT')\n" +
                "where case_id = ? /*:case_id*/\n", caseId);

        while (rs.next()) paramFromSqlDb = rs.getString("val");

        //result query
        String xQuery = "let $i := :in/docs/doc \n" +
                "let $x := \n" +
                paramFromSqlDb +
                "for $j in $x//service\n" +
                "for $k in $i\n" +
                "where $j/path = $k/path\n" +
                "and $k//content[./archetype_details/archetype_id/value='openEHR-EHR-EVALUATION.assignments_and_recommendations.v1']/data[@archetype_node_id='at0001']/items[@archetype_node_id='at0002']/value/value/text() != ''\n" +
                "return \n" +
                "<services>\n" +
                "{$j/name}\n" +
                "{$j/date}\n" +
                "\n" +
                "<conclusion>\n" +
                "{$k//content[./archetype_details/archetype_id/value='openEHR-EHR-EVALUATION.assignments_and_recommendations.v1']/data[@archetype_node_id='at0001']/items[@archetype_node_id='at0002']/value/value/text()}\n" +
                "</conclusion>\n" +
                "\n" +
                "\n" +
                "</services>";

        String result = conn.queryXml(docs, xQuery);
        return util.xmlToJSON(result);
    }

    public String query12(Integer caseId){
        FilledProtocolStorage storage = new FilledProtocolStorage();
        storage.setRoot(repoPath);
        ComplexXmlConnection conn = new ComplexXmlConnection();
        conn.setFileStorage(storage);
        conn.setReadOnlyXmlDataSource(readOnlyXmlDataSource);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        Util util = new Util();
        List<String> fileList = new ArrayList<>();
        String paramFromSqlDb = "";

        //getting protocolList
        SqlRowSet rs = jdbcTemplate.queryForRowSet("select  \n" +
                " p.path\n" +
                "from md_srv_rendered mr\n" +
                "join sr_srv_rendered r on r.id = mr.id\n" +
                "join sr_service s on s.id = r.service_id\n" +
                "join sr_srv_type st on st.id = s.type_id and st.code = 'SURGERY'\n" +
                "join md_srv_protocol sp on sp.srv_rendered_id = r.id\n" +
                "join md_ehr_protocol p on sp.protocol_id = p.id \n" +
                "where case_id = ? /*876804 :case_id*/", caseId);
        while (rs.next())
            fileList.add(rs.getString("path"));

        String docs = util.getDocumentsWithPaths(fileList, conn);

        //getting paramFromSqlDb
        rs = jdbcTemplate.queryForRowSet("select  \n" +
                " '<services>' || string_agg('<service><date>' || to_char(r.bdate, 'dd.mm.yyyy') || '</date>' || '<path>' || p.path || '</path>' || '<name>' || s.name || '</name></service>', '') || '</services>' val\n" +
                "from md_srv_rendered mr\n" +
                "join sr_srv_rendered r on r.id = mr.id\n" +
                "join sr_service s on s.id = r.service_id\n" +
                "join sr_srv_type st on st.id = s.type_id and st.code = 'SURGERY'\n" +
                "join md_srv_protocol sp on sp.srv_rendered_id = r.id\n" +
                "join md_ehr_protocol p on sp.protocol_id = p.id \n" +
                "where case_id = ? /*876804 :case_id*/", caseId);

        while (rs.next()) paramFromSqlDb = rs.getString("val");

        //result query
        String xQuery = "let $i := :in/docs/doc \n" +
                "let $x := \n" +
                paramFromSqlDb +
                "for $j in $x//service\n" +
                "for $k in $i\n" +
                "where $j/path = $k/path\n" +
                "and $k//content[./archetype_details/archetype_id/value='openEHR-EHR-OBSERVATION.operation_description.v1']/data[@archetype_node_id='at0001']/events[@archetype_node_id='at0002']/data[@archetype_node_id='at0003']/items[@archetype_node_id='at0004']/value/value/text() != ''\n" +
                "return \n" +
                "<services>\n" +
                "{$j/name}\n" +
                "{$j/date}\n" +
                "\n" +
                "<conclusion>\n" +
                "{$k//content[./archetype_details/archetype_id/value='openEHR-EHR-OBSERVATION.operation_description.v1']/data[@archetype_node_id='at0001']/events[@archetype_node_id='at0002']/data[@archetype_node_id='at0003']/items[@archetype_node_id='at0004']/value/value/text()}\n" +
                "</conclusion>\n" +
                "\n" +
                "\n" +
                "</services>";

        String result = conn.queryXml(docs, xQuery);
        //todo check 2 operations
        return util.xmlToJSON(result);
    }

    public String query13a(Integer caseId){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String r = "";
        SqlRowSet rs =
                jdbcTemplate.queryForRowSet("with t as (\n" +
                        "select \n" +
                        " case when count(1) > 1 then 'Комплекс: ' ||\n" +
                        "    string_agg(\n" +
                        "      concat(h.name, case when ph.is_patient_medicament then ' (собст.) ' else '' end, ' ', pp.name, ' ', ar.short_name, ' (' || ph.hold_dose_value, ' ', dm.mnemocode, ') ', extract(day from age(p.period_end_dt, p.period_begin_dt)) || ' дн.'), '; '\n" +
                        "    )\n" +
                        " else\n" +
                        "    string_agg(\n" +
                        "      concat(h.name, case when ph.is_patient_medicament then ' (собст.) ' else '' end, ' ', pp.name, ' ', ar.short_name, ' (' || ph.hold_dose_value, ' ', dm.mnemocode, ') ', extract(day from age(p.period_end_dt, p.period_begin_dt)) || ' дн.'), chr(13)\n" +
                        "    )\n" +
                        " end val\n" +
                        "\n" +
                        "from hospital.prescription p \n" +
                        "join hospital.prescription_holding ph on p.id = ph.prescription_id --and p.status_id = 4\n" +
                        "join hospital.prescription_periodicity pp on pp.id = p.periodicity_id\n" +
                        "join inventory.holding h on h.id = ph.holding_id\n" +
                        "join cmn_measure dm on dm.id = ph.hold_dose_measure_id\n" +
                        "join md_administration_route ar on ar.id = p.administration_route_id\n" +
                        "where p.case_id = ?\n" +
                        "group by p.id\n" +
                        "),\n" +
                        "\n" +
                        "t1 as (\n" +
                        "select \n" +
                        " case when count(1) > 1 then 'Комплекс: ' ||\n" +
                        "    string_agg(\n" +
                        "      concat(h.name, case when ph.is_patient_medicament then ' (собст.) ' else '' end, ' ', pp.name, ' ', ar.short_name, ' (' || ph.hold_dose_value, ' ', dm.mnemocode, ') ', extract(day from age(p.period_end_dt, p.period_begin_dt)) || ' дн. '), '; '\n" +
                        "    ) || ' дата отмены:' || to_char(min(cancel_dt), 'dd.mm.yyyy') || ' Причина отмены:' || min(cancel_reason)\n" +
                        " else\n" +
                        "    string_agg(\n" +
                        "      concat(h.name, case when ph.is_patient_medicament then ' (собст.) ' else '' end, ' ', pp.name, ' ', ar.short_name, ' (' || ph.hold_dose_value, ' ', dm.mnemocode, ') ', extract(day from age(p.period_end_dt, p.period_begin_dt)) || ' дн.' || ' дата отмены:' || to_char(cancel_dt, 'dd.mm.yyyy') || ' Причина отмены:' || cancel_reason), chr(13)\n" +
                        "    )\n" +
                        " end val\n" +
                        "\n" +
                        "from hospital.prescription p \n" +
                        "join hospital.prescription_holding ph on p.id = ph.prescription_id and p.status_id = 5\n" +
                        "join hospital.prescription_periodicity pp on pp.id = p.periodicity_id\n" +
                        "join inventory.holding h on h.id = ph.holding_id\n" +
                        "join cmn_measure dm on dm.id = ph.hold_dose_measure_id\n" +
                        "join md_administration_route ar on ar.id = p.administration_route_id\n" +
                        "where p.case_id = ?\n" +
                        "group by p.id\n" +
                        ")\n" +
                        "\n" +
                        "select\n" +
                        " concat((select string_agg(val, chr(13)) from t), chr(13) || 'Отменено:' || chr(13) || (select string_agg(val, chr(13)) from t1)) val", caseId, caseId);

        while (rs.next()) r = rs.getString("val");
        return r;
    }

    public String query13b(Integer caseId){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String r = "";
        SqlRowSet rs =
                jdbcTemplate.queryForRowSet("select string_agg(\n" +
                        "  concat(s.name, ' ', pp.name, ' (' || case when ps.duration is not null or m.mnemocode is not null then trim(concat(ps.duration || ' ', m.mnemocode)) end || ')'), chr(13)\n" +
                        ") val\n" +
                        "from hospital.prescription p \n" +
                        "join hospital.prescription_service ps on ps.prescription_id = p.id and p.status_id = 4\n" +
                        "join sr_service s on s.id = ps.service_id\n" +
                        "join sr_srv_type st on st.id = s.type_id and st.code = 'PROCEDURE'\n" +
                        "join hospital.prescription_periodicity pp on pp.id = p.periodicity_id\n" +
                        "left join cmn_measure m on m.id = ps.duration_measure_unit_id\n" +
                        "where case_id = ?", caseId);

        while (rs.next()) r = rs.getString("val");
        return r;
    }

    public String query14a(Integer caseId){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String r = "";
        SqlRowSet rs =
                jdbcTemplate.queryForRowSet("select \n" +
                        " string_agg(\n" +
                        "   concat(h.name, ' ', pp.name, ' ', ar.short_name, ' (' || ph.hold_dose_value, ' ', dm.mnemocode, ') ', extract(day from age(p.period_end_dt, p.period_begin_dt)) || ' дн.'), chr(13)\n" +
                        " ) val\n" +
                        "from hospital.prescription p \n" +
                        "join hospital.prescription_holding ph on p.id = ph.prescription_id and p.status_id = 2\n" +
                        "join hospital.prescription_periodicity pp on pp.id = p.periodicity_id\n" +
                        "join inventory.holding h on h.id = ph.holding_id\n" +
                        "join cmn_measure dm on dm.id = ph.hold_dose_measure_id\n" +
                        "left join md_administration_route ar on ar.id = p.administration_route_id\n" +
                        "where p.case_id = ? /*885652 :case_id*/", caseId);

        while (rs.next()) r = rs.getString("val");
        return r;
    }

    public String query14b(Integer caseId){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String r = "";
        SqlRowSet rs =
                jdbcTemplate.queryForRowSet("select \n" +
                        " string_agg(\n" +
                        "  concat(s.name, ' ', pp.name, ' (' || case when ps.duration is not null or m.mnemocode is not null then trim(concat(ps.duration || ' ', m.mnemocode)) end || ')'), chr(13)\n" +
                        ") val\n" +
                        "from hospital.prescription p \n" +
                        "join hospital.prescription_service ps on ps.prescription_id = p.id and p.status_id = 2\n" +
                        "join sr_service s on s.id = ps.service_id\n" +
                        "join sr_srv_type st on st.id = s.type_id and st.code = 'PROCEDURE'\n" +
                        "join hospital.prescription_periodicity pp on pp.id = p.periodicity_id\n" +
                        "left join cmn_measure m on m.id = ps.duration_measure_unit_id\n" +
                        "where case_id = ? /*885652 :case_id*/", caseId);

        while (rs.next()) r = rs.getString("val");
        return r;
    }

    public String query16(Integer stepId){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String r = "";
        SqlRowSet rs =
                jdbcTemplate.queryForRowSet("select trim(concat(trim(concat(i.surname, ' ', i.name)), ' ', i.patr_name)) || ' (' || pp.name || ')' val\n" +
                        "from mc_step s\n" +
                        "join sr_res_group rg on rg.id = s.res_group_id\n" +
                        "join pim_employee_position ep on ep.id = rg.responsible_id \n" +
                        "join pim_employee e on e.id = ep.employee_id\n" +
                        "join pim_individual i on i.id = e.individual_id \n" +
                        "join pim_position pp on pp.id = ep.position_id\n" +
                        "where s.id = ? limit 1", stepId);

        while (rs.next()) r = rs.getString("val");
        return r;
    }

    public String query17(Integer caseId){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String r = "";
        SqlRowSet rs =
                jdbcTemplate.queryForRowSet("select coalesce(o.short_name, o.full_name) val\n" +
                        "from mc_case c\n" +
                        "join md_referral r on r.id = c.referral_id\n" +
                        "join pim_organization o on r.ref_organization_id = o.id\n" +
                        "where c.id = ?", caseId);

        while (rs.next()) r = rs.getString("val");
        return r;
    }

    public String query18(Integer caseId){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String r = "";
        SqlRowSet rs =
                jdbcTemplate.queryForRowSet("select concat(to_char(death_date, 'dd.mm.yyyy'), ' ' || death_time::text) val\n" +
                        "from mc_step st where death_date is not null and case_id = ?\n" +
                        "limit 1", caseId);

        while (rs.next()) r = rs.getString("val");
        return r;
    }

    public String query20(Integer caseId){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String r = "";
        SqlRowSet rs = jdbcTemplate.queryForRowSet("select death_reason val from mc_case where id = ?", caseId);
        while (rs.next()) r = rs.getString("val");
        return r;
    }

    public String query21(Integer caseId){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String r = "";
        SqlRowSet rs = jdbcTemplate.queryForRowSet("with recursive t as (\n" +
                " select s.id, hr.previous_id, hr.department_id from mc_step s \n" +
                " join hsp_record hr on hr.id = s.id\n" +
                " where s.case_id = ? /*:case_id*/\n" +
                "),\n" +
                "\n" +
                "ls as (\n" +
                " select t.id, t.previous_id, t.department_id from t\n" +
                " left join t t1 on t1.previous_id = t.id \n" +
                " where t1.id is null\n" +
                " limit 1\n" +
                "),\n" +
                "\n" +
                "x as (\n" +
                " select ls.id, 0 rn, s.reason_id, ls.previous_id, ls.department_id, false dep_change from ls join mc_step s on s.id = ls.id union all\n" +
                " select hr.id, rn + 1, s.reason_id, hr.previous_id, hr.department_id, hr.department_id != x.department_id  from hsp_record hr join x on x.previous_id = hr.id join mc_step s on s.id = hr.id\n" +
                ")\n" +
                "\n" +
                "select rr.name val from x \n" +
                "join mc_step_result_reason rr on rr.id = x.reason_id and dep_change\n" +
                "order by rn limit 1", caseId);
        while (rs.next()) r = rs.getString("val");
        return r;
    }

    public String query22(Integer caseId){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String r = "";
        SqlRowSet rs = jdbcTemplate.queryForRowSet("with t as (\n" +
                " select s.id, hr.previous_id, s.outcome_id from mc_step s \n" +
                " join hsp_record hr on hr.id = s.id\n" +
                " where s.case_id = ? /*:case_id*/\n" +
                "),\n" +
                "\n" +
                "ls as (\n" +
                " select t.id, t.previous_id, t.outcome_id from t\n" +
                " left join t t1 on t1.previous_id = t.id \n" +
                " where t1.id is null\n" +
                " limit 1\n" +
                ")\n" +
                "\n" +
                "select cr.name val from ls\n" +
                "join mc_step_care_result cr on ls.outcome_id = cr.id", caseId);
        while (rs.next()) r = rs.getString("val");
        return r;
    }

    public String query24(Integer caseId, Integer epicrisisTypeId){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String r = "";
        SqlRowSet rs = jdbcTemplate.queryForRowSet("with recursive t as (\n" +
                " select hr1.id, hr1.department_id d_id, d1.name dn, s1.admission_date ad, s1.outcome_date od\n" +
                " from mc_step s \n" +
                " join hsp_record hr on hr.id = s.id\n" +
                " join hsp_record hr1 on hr1.previous_id = hr.id\n" +
                " join mc_step s1 on s1.id = hr1.id\n" +
                " join pim_department d1 on d1.id = hr1.department_id\n" +
                " where s.case_id = ? /*:case_id*/ and hr.previous_id is null\n" +
                "),\n" +
                "\n" +
                "r as (\n" +
                " select id, d_id, dn, ad, od, 1 zog from t \n" +
                "  union all\n" +
                " select hr.id, d.id, d.name, s.admission_date, s.outcome_date, case when hr.department_id != r.d_id then zog + 1 else zog end\n" +
                " from r join hsp_record hr on hr.previous_id = r.id \n" +
                " join mc_step s on s.id = hr.id \n" +
                " join pim_department d on d.id = hr.department_id\n" +
                "),\n" +
                "\n" +
                "res as (\n" +
                " select min(dn) dn, min(ad) ad, max(coalesce(od, 'infinity')) od, zog from r group by zog order by zog\n" +
                "),\n" +
                "\n" +
                "res1 as (\n" +
                " select concat(dn, ' ', to_char(ad, 'dd.mm.yyyy'), ' - ', case when od = 'infinity' then 'по настоящее время' else to_char(od, 'dd.mm.yyyy') end) val, row_number()over(order by zog) rn, row_number()over(order by zog desc) rn1 from res \n" +
                ")\n" +
                "\n" +
                "\n" +
                "select \n" +
                " 'Дата госпитализации: ' || to_char(ad, 'dd.mm.yyyy') || ' ' || dn || chr(13) || \n" +
                " case when et.code in ('4', '5') then (select string_agg(val, chr(13)) from (select val from res1 order by rn) x) \n" +
                "      else (select string_agg(val, chr(13)) from res1 where rn1 = 1) \n" +
                " end val\n" +
                "from t\n" +
                "join mc_epicrisis_type et on et.id = ? /*:epicrisis_type*/", caseId, epicrisisTypeId);
        while (rs.next()) r = rs.getString("val");
        return r;
    }

    public String query25(Integer caseId){
        FilledProtocolStorage storage = new FilledProtocolStorage();
        storage.setRoot(repoPath);
        ComplexXmlConnection conn = new ComplexXmlConnection();
        conn.setFileStorage(storage);
        conn.setReadOnlyXmlDataSource(readOnlyXmlDataSource);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        Util util = new Util();
        List<String> fileList = new ArrayList<>();
        String paramFromSqlDb = "";

        //getting protocolList
        SqlRowSet rs = jdbcTemplate.queryForRowSet("select p.path from sr_srv_rendered r \n" +
                "join md_srv_rendered mr on mr.id = r.id\n" +
                "join md_srv_protocol sp on sp.srv_rendered_id = r.id\n" +
                "join md_ehr_protocol p on sp.protocol_id = p.id\n" +
                "where case_id = ? and r.bdate <= current_date and p.path not like '2015%'", caseId);
        while (rs.next())
            fileList.add(rs.getString("path"));

        String docs = util.getDocumentsWithPaths(fileList, conn);

        //getting paramFromSqlDb
        rs = jdbcTemplate.queryForRowSet("select '<services>' || string_agg(concat('<service><path>', p.path, '</path>', '<bdate>', to_char(r.bdate, 'yyyy-mm-dd'), '</bdate></service>'), '') || '</services>'  val from sr_srv_rendered r \n" +
                "join md_srv_rendered mr on mr.id = r.id\n" +
                "join md_srv_protocol sp on sp.srv_rendered_id = r.id\n" +
                "join md_ehr_protocol p on sp.protocol_id = p.id\n" +
                "where case_id = ? and r.bdate <= current_date and p.path not like '2015%'", caseId);

        while (rs.next()) paramFromSqlDb = rs.getString("val");

        //result query
        String xQuery = "(let $i := :in/docs/doc \n" +
                "let $x := \n" +
                paramFromSqlDb +
                "for $j in $x//service \n" +
                " for $k in $i \n" +
                "  where $j/path = $k/path \n" +
                "  and $k/data/content[.//archetype_id='openEHR-EHR-OBSERVATION.general_condition.v1']/data[@archetype_node_id='at0001']/events[@archetype_node_id='at0002']/data[@archetype_node_id='at0003']/items[@archetype_node_id='at0004']/value/value/text() != ''\n" +
                "   order by $j/bdate descending\n" +
                "return $k/data/content[.//archetype_id='openEHR-EHR-OBSERVATION.general_condition.v1']/data[@archetype_node_id='at0001']/events[@archetype_node_id='at0002']/data[@archetype_node_id='at0003']/items[@archetype_node_id='at0004']/value/value/text())[position() = 1]";

        return conn.queryXml(docs, xQuery);
    }

    public String query0(Integer patientId, Integer caseId){
        FilledProtocolStorage storage = new FilledProtocolStorage();
        storage.setRoot(repoPath);
        ComplexXmlConnection conn = new ComplexXmlConnection();
        conn.setFileStorage(storage);
        conn.setReadOnlyXmlDataSource(readOnlyXmlDataSource);
        Util util = new Util();
        List<String> fileList = new ArrayList<>();
        String paramFromSqlDb = "";

        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String result = "";
        SqlRowSet rs =
                jdbcTemplate.queryForRowSet("with t as (\n" +
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
                        "from pim_individual i where i.id = ? /*:patient_id*/\n" +
                        ")\n" +
                        "\n" +
                        "select \n" +
                        " concat('\"pat_name\": \"', fio, \n" +
                        "  '\",\"pat_birth_dt\":\"' || \n" +
                        "  case when birth_dt is not null then\n" +
                        "    case when full_years < 18 then full_years || ' л. ' || \n" +
                        "      case when full_months != 0 then full_months || ' мес. ' else '' end\n" +
                        "    else full_years || ' л.' end \n" +
                        "  else \n" +
                        "    'не указан'   \n" +
                        "  end, '\", \"pat_job\":\"', ppj, '\"'\n" +
                        "  ) val\n" +
                        " \n" +
                        "from t limit 1", patientId);

        while (rs.next()) result += rs.getString("val");

        //doc
        rs = jdbcTemplate.queryForRowSet("select \n" +
                        "concat(\n" +
                        " ',\"doc_series\":\"' || id.series || '\"',\n" +
                        " ',\"doc_number\":\"' || id.number || '\"',\n" +
                        " ',\"doc_type\":\"' || dt.name || '\"') val\n" +
                        "\n" +
                        "from pim_individual i\n" +
                        "join pim_individual_doc id on i.id = id.indiv_id and id.is_active and current_date between coalesce(id.issue_dt, '-infinity') and coalesce(id.expire_dt, 'infinity') \n" +
                        "join pim_doc_type dt on dt.id = id.type_id\n" +
                        "join pim_doc_type_category dtc on dtc.type_id = dt.id and dtc.category_id = 1\n" +
                        "where i.id = ? order by case dt.code when 'PASSPORT_RUSSIAN_FEDERATION' then 0 else 1 end limit 1", patientId);

        while (rs.next()) result += rs.getString("val");

        //diagnosis
        rs = jdbcTemplate.queryForRowSet("with t as (\n" +
                " select \n" +
                "  concat(s.name || ' - ', d.code, ' (', d.name, ')' ) d\n" +
                " from mc_diagnosis md \n" +
                " join md_diagnosis d on d.id = md.diagnos_id\n" +
                " join mc_stage s on s.id = md.stage_id\n" +
                " left join mc_diagnosis_type dt on dt.id = md.type_id\n" +
                " where md.case_id = ? /*:case_id*/\n" +
                " order by s.stage_order, md.establishment_date, dt.id\n" +
                ")\n" +
                "\n" +
                "select ',\"diagnosis\":[' || string_agg('\"' || d || '\"', ',') || ']' val from t limit 1", caseId);

        while (rs.next()) {
            String val = rs.getString("val");
            if (val != null) result += val;
        }

        //anamnesis
        //getting protocolList
        rs = jdbcTemplate.queryForRowSet("select\n" +
                " path \n" +
                "from md_srv_rendered mr\n" +
                "join sr_srv_rendered r on r.id = mr.id\n" +
                "join sr_service s on s.id = r.service_id \n" +
                "join sr_srv_type st on st.id = s.type_id \n" +
                "join md_srv_protocol sp on sp.srv_rendered_id = r.id\n" +
                "join md_ehr_protocol p on sp.protocol_id = p.id\n" +
                "where case_id = ?", caseId);
        while (rs.next())
            fileList.add(rs.getString("path"));

        String docs = util.getDocumentsWithPaths(fileList, conn);

        //getting paramFromSqlDb
        rs = jdbcTemplate.queryForRowSet("select \n" +
                "'<services>' || string_agg(concat('<service><path>', p.path, '</path>', '<bdate>', to_char(r.bdate, 'yyyy-mm-dd'), '</bdate></service>'), '') || '</services>'  val\n" +
                "from md_srv_rendered mr \n" +
                "\n" +
                "join sr_srv_rendered r on r.id = mr.id\n" +
                "join md_srv_protocol sp on sp.srv_rendered_id = r.id\n" +
                "join md_ehr_protocol p on sp.protocol_id = p.id \n" +
                "where mr.case_id = ?\n", caseId);

        while (rs.next()) paramFromSqlDb = rs.getString("val");

        //result query
        if (fileList.size() > 0 && paramFromSqlDb != null) {
            String xQuery = "(let $i := :in/docs/doc \n" +
                    "let $x := \n" +
                    paramFromSqlDb +
                    "for $j in $x//service \n" +
                    " for $k in $i \n" +
                    "  where $j/path = $k/path \n" +
                    "  and $k/data/content[.//archetype_id='openEHR-EHR-OBSERVATION.anamnesis_morbi.v1']/data[@archetype_node_id='at0001']/events[@archetype_node_id='at0002']/data[@archetype_node_id='at0003']/items[@archetype_node_id='at0004']/value/value/text() != ''\n" +
                    "   order by $j/bdate \n" +
                    "return concat(',\"anamnesis\":\"', $k/data/content[.//archetype_id='openEHR-EHR-OBSERVATION.anamnesis_morbi.v1']/data[@archetype_node_id='at0001']/events[@archetype_node_id='at0002']/data[@archetype_node_id='at0003']/items[@archetype_node_id='at0004']/value/value/text(), '\"'))[position() = 1]";

            result += conn.queryXml(docs, xQuery);
        }
        return result;

    }
}
