package org.dao;

import cz.atria.lsd.md.ehr.xmldb.complex.ComplexXmlConnection;
import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Service;

import java.io.IOException;
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
                "from t", patientId);

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

        String result = conn.queryXml(docs, xQuery);

        return result;
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

        String result = conn.queryXml(docs, xQuery);

        return result;
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

    public String query9(Integer caseId) throws IOException {
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

}
